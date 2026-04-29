package com.example.archive.archive;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;

/**
 * Streams a partition's rows out via PostgreSQL's COPY protocol, gzip-compresses
 * them in memory, and uploads the resulting CSV.gz to S3. Buffering in memory
 * (rather than streaming through a piped publisher) keeps the PoC tractable —
 * daily partitions in a typical demo are well under 100 MB compressed. A future
 * variant for very large partitions could swap in the awssdk transfer manager
 * with multipart uploads; the {@link PartitionArchiver} interface is the seam.
 */
@Service
public class SpringJdbcS3Archiver implements PartitionArchiver {

    private static final Logger log = LoggerFactory.getLogger(SpringJdbcS3Archiver.class);

    private final DataSource dataSource;
    private final S3Client s3Client;
    private final ArchiveProperties props;

    public SpringJdbcS3Archiver(DataSource dataSource, S3Client s3Client, ArchiveProperties props) {
        this.dataSource = dataSource;
        this.s3Client = s3Client;
        this.props = props;
    }

    @Override
    public ArchiveResult archive(PartitionInfo partition) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        long rowCount;
        Connection conn = DataSourceUtils.getConnection(dataSource);
        try {
            PGConnection pg = conn.unwrap(PGConnection.class);
            CopyManager copyManager = pg.getCopyAPI();
            String sql = "COPY (SELECT * FROM " + quoteIdent(partition.tableName())
                    + ") TO STDOUT WITH (FORMAT csv, HEADER)";
            try (GZIPOutputStream gz = new GZIPOutputStream(buffer)) {
                rowCount = copyManager.copyOut(sql, gz);
            }
        } catch (SQLException | IOException e) {
            throw new ArchiveException("COPY failed for " + partition.tableName(), e);
        } finally {
            DataSourceUtils.releaseConnection(conn, dataSource);
        }

        byte[] gzipBytes = buffer.toByteArray();
        String key = s3Key(partition);
        log.info("Uploading partition {} as s3://{}/{} ({} rows, {} bytes)",
                partition.tableName(), props.bucket(), key, rowCount, gzipBytes.length);
        s3Client.putObject(b -> b.bucket(props.bucket())
                        .key(key)
                        .contentType("application/gzip"),
                RequestBody.fromBytes(gzipBytes));

        HeadObjectResponse head;
        try {
            head = s3Client.headObject(b -> b.bucket(props.bucket()).key(key));
        } catch (NoSuchKeyException e) {
            throw new ArchiveException("S3 object missing after PUT for " + partition.tableName(), e);
        }
        if (head.contentLength() == null || head.contentLength() == 0) {
            throw new ArchiveException("S3 object is empty for " + partition.tableName());
        }
        return new ArchiveResult(props.bucket(), key, rowCount, gzipBytes.length);
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
}
