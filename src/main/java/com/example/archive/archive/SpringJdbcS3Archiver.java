package com.example.archive.archive;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.Upload;

import javax.sql.DataSource;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

/**
 * Streams a partition's rows out via PostgreSQL's COPY protocol, gzip-compresses
 * them on the fly, and uploads the result to S3 via the AWS CRT-backed
 * {@link S3TransferManager}. There is no in-memory buffer of the full payload —
 * partitions are bounded only by Postgres-side limits and S3's 5 TiB single-object
 * ceiling.
 *
 * <p>Pipeline: COPY → {@link GZIPOutputStream} → {@link PipedOutputStream} →
 * {@link PipedInputStream} → Transfer Manager. The COPY runs on a producer
 * thread; the SDK reads the piped input from its own executor. A slow link
 * backpressures by stalling COPY's next gzip write — the right direction.
 */
@Service
@ConditionalOnProperty(name = "archive.format", havingValue = "csv", matchIfMissing = true)
public class SpringJdbcS3Archiver implements PartitionArchiver {

    private static final Logger log = LoggerFactory.getLogger(SpringJdbcS3Archiver.class);

    /** PipedStream default is 1 KiB; bumping reduces producer/consumer ping-pong on every chunk. */
    private static final int PIPE_BUFFER_BYTES = 64 * 1024;

    private final DataSource dataSource;
    private final S3TransferManager transferManager;
    private final ArchiveProperties props;

    public SpringJdbcS3Archiver(DataSource dataSource,
                                S3TransferManager transferManager,
                                ArchiveProperties props) {
        this.dataSource = dataSource;
        this.transferManager = transferManager;
        this.props = props;
    }

    @Override
    public ArchiveResult archive(PartitionInfo partition) {
        String key = s3Key(partition);
        String sql = "COPY (SELECT * FROM " + quoteIdent(partition.tableName())
                + ") TO STDOUT WITH (FORMAT csv, HEADER)";

        // Two short-lived executors: one to run COPY (producer), one for the SDK to
        // pull bytes from the piped input. Both are torn down once the upload finishes.
        ExecutorService copyExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "archive-copy-" + partition.tableName());
            t.setDaemon(true);
            return t;
        });
        ExecutorService readerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "archive-read-" + partition.tableName());
            t.setDaemon(true);
            return t;
        });

        try {
            PipedInputStream pis = new PipedInputStream(PIPE_BUFFER_BYTES);
            PipedOutputStream pos = new PipedOutputStream(pis);

            CompletableFuture<CopyResult> producer = CompletableFuture.supplyAsync(
                    () -> runCopy(partition.tableName(), sql, pos), copyExecutor);

            // Null content length tells the CRT client "single-part PUT or multipart;
            // figure it out from what flows through". The reader runs on readerExecutor.
            AsyncRequestBody body = AsyncRequestBody.fromInputStream(pis, null, readerExecutor);

            Upload upload = transferManager.upload(b -> b
                    .putObjectRequest(p -> p
                            .bucket(props.bucket())
                            .key(key)
                            .contentType("application/gzip"))
                    .requestBody(body));

            CopyResult copy;
            CompletedUpload completed;
            try {
                copy = producer.join();
                completed = upload.completionFuture().join();
            } catch (CompletionException | CancellationException e) {
                upload.completionFuture().cancel(true);
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                if (cause instanceof ArchiveException ae) throw ae;
                throw new ArchiveException("Archive failed for " + partition.tableName(), cause);
            }

            log.info("Streamed partition {} to s3://{}/{} ({} rows, {} compressed bytes, etag {})",
                    partition.tableName(), props.bucket(), key,
                    copy.rowCount, copy.compressedBytes, completed.response().eTag());

            return new ArchiveResult(props.bucket(), key, copy.rowCount, copy.compressedBytes);
        } catch (IOException e) {
            throw new ArchiveException("Could not open pipe for " + partition.tableName(), e);
        } finally {
            copyExecutor.shutdown();
            readerExecutor.shutdown();
        }
    }

    private CopyResult runCopy(String partitionName, String sql, PipedOutputStream pos) {
        Connection conn = DataSourceUtils.getConnection(dataSource);
        try {
            PGConnection pg = conn.unwrap(PGConnection.class);
            CopyManager copyManager = pg.getCopyAPI();
            // pos is closed by try-with-resources — that's what signals end-of-stream
            // to the AsyncRequestBody reader on the other side of the pipe.
            try (var counter = new CountingOutputStream(pos);
                 var gz = new GZIPOutputStream(counter)) {
                long rowCount = copyManager.copyOut(sql, gz);
                gz.finish();
                return new CopyResult(rowCount, counter.getCount());
            }
        } catch (SQLException | IOException e) {
            throw new ArchiveException("COPY failed for " + partitionName, e);
        } finally {
            DataSourceUtils.releaseConnection(conn, dataSource);
        }
    }

    /**
     * Hive-style key: events/year=YYYY/month=MM/day=DD/events-{partitionTable}.csv.gz.
     * Day comes from the partition's lower bound (in UTC), so the key cleanly aligns
     * with the partition's logical date even if the host is in another timezone.
     */
    String s3Key(PartitionInfo partition) {
        var lb = partition.lowerBound();
        return String.format(Locale.ROOT,
                "events/year=%04d/month=%02d/day=%02d/events-%s.csv.gz",
                lb.getYear(), lb.getMonthValue(), lb.getDayOfMonth(), partition.tableName());
    }

    /**
     * Identifier quoting per Postgres rules. Partition names come from pg_class.relname
     * (not user input), but defensive quoting keeps the SQL builder robust if a future
     * caller hands in a less-trusted source.
     */
    private static String quoteIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private record CopyResult(long rowCount, long compressedBytes) {}

    /** Tallies bytes written so we can report compressed size in {@link ArchiveResult}. */
    private static final class CountingOutputStream extends FilterOutputStream {
        private long count;

        CountingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            count += len;
        }

        long getCount() {
            return count;
        }
    }
}
