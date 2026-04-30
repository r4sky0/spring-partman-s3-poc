package com.example.archive.archive;

import com.jerolba.carpet.CarpetWriter;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.Upload;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

/**
 * Exports a partition as Apache Parquet (columnar, compressed, directly
 * queryable by Athena/DuckDB/Spark from S3) instead of gzipped CSV. Selected
 * via {@code archive.format=parquet}.
 *
 * <p>Uses Carpet ({@link com.jerolba.carpet.CarpetWriter}) for the write path —
 * a Java-records-based wrapper around parquet-java that absorbs the Hadoop
 * dependency dance into its own pom, so this module's pom stays clean of
 * hadoop-common, spring-boot-maven-plugin exclusions, etc.
 *
 * <p>The writer assembles the file on local disk first, then hands it to the
 * Transfer Manager. Disk I/O is dwarfed by S3 upload time for any non-trivial
 * partition; streaming Parquet through a pipe would mean buffering the footer
 * separately and is more trouble than it's worth here.
 */
@Service
@ConditionalOnProperty(name = "archive.format", havingValue = "parquet")
public class ParquetS3Archiver implements PartitionArchiver {

    private static final Logger log = LoggerFactory.getLogger(ParquetS3Archiver.class);

    /**
     * One row from {@code public.events}. Carpet derives the Parquet schema from
     * this record's component types; mapping is straightforward except for two
     * Postgres specifics:
     * <ul>
     *   <li>{@code payload} (JSONB) lands as a Parquet STRING — analytics
     *       consumers re-parse JSON on read.</li>
     *   <li>{@code createdAt} (TIMESTAMPTZ) is converted to {@link Instant} so
     *       Carpet writes it as int64 with logical type {@code timestamp-micros}.</li>
     * </ul>
     */
    public record EventRow(long id, UUID tenantId, String eventType, String payload, Instant createdAt) {}

    private final JdbcTemplate jdbcTemplate;
    private final S3TransferManager transferManager;
    private final ArchiveProperties props;

    public ParquetS3Archiver(JdbcTemplate jdbcTemplate,
                             S3TransferManager transferManager,
                             ArchiveProperties props) {
        this.jdbcTemplate = jdbcTemplate;
        this.transferManager = transferManager;
        this.props = props;
    }

    @Override
    public ArchiveResult archive(PartitionInfo partition) {
        String key = s3Key(partition);
        String selectSql = "SELECT id, tenant_id, event_type, payload, created_at FROM "
                + quoteIdent(partition.tableName());

        Path tempFile;
        try {
            tempFile = Files.createTempFile("partition-" + partition.tableName() + "-", ".parquet");
        } catch (IOException e) {
            throw new ArchiveException("Could not create temp file for " + partition.tableName(), e);
        }

        long rowCount;
        long bytesUploaded;
        try {
            rowCount = writeParquet(selectSql, tempFile);
            bytesUploaded = Files.size(tempFile);
            log.info("Uploading partition {} as s3://{}/{} ({} rows, {} bytes parquet)",
                    partition.tableName(), props.bucket(), key, rowCount, bytesUploaded);

            Upload upload = transferManager.upload(b -> b
                    .putObjectRequest(p -> p
                            .bucket(props.bucket())
                            .key(key)
                            .contentType("application/vnd.apache.parquet"))
                    .requestBody(AsyncRequestBody.fromFile(tempFile)));

            CompletedUpload completed = upload.completionFuture().join();
            log.debug("S3 ETag {} for key {}", completed.response().eTag(), key);
        } catch (CompletionException | CancellationException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new ArchiveException("S3 upload failed for " + partition.tableName(), cause);
        } catch (IOException e) {
            throw new ArchiveException("Could not stat temp file for " + partition.tableName(), e);
        } finally {
            deleteQuietly(tempFile);
        }

        return new ArchiveResult(props.bucket(), key, rowCount, bytesUploaded);
    }

    private long writeParquet(String selectSql, Path tempFile) {
        return jdbcTemplate.execute((java.sql.Connection conn) -> {
            try (var stmt = conn.prepareStatement(selectSql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                stmt.setFetchSize(1000);
                try (ResultSet rs = stmt.executeQuery();
                     OutputStream os = Files.newOutputStream(tempFile);
                     CarpetWriter<EventRow> writer = new CarpetWriter<>(os, EventRow.class)) {
                    long rows = 0;
                    while (rs.next()) {
                        writer.write(toRow(rs));
                        rows++;
                    }
                    return rows;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static EventRow toRow(ResultSet rs) throws SQLException {
        OffsetDateTime createdAt = rs.getObject("created_at", OffsetDateTime.class);
        Object payloadObj = rs.getObject("payload");
        String payload = switch (payloadObj) {
            case null -> null;
            case PGobject pg -> pg.getValue();
            case String s -> s;
            default -> payloadObj.toString();
        };
        return new EventRow(
                rs.getLong("id"),
                rs.getObject("tenant_id", UUID.class),
                rs.getString("event_type"),
                payload,
                createdAt == null ? null : createdAt.toInstant());
    }

    private static void deleteQuietly(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignored) {
            // best-effort cleanup; the OS will reap on reboot
        }
    }

    String s3Key(PartitionInfo partition) {
        var lb = partition.lowerBound();
        return String.format(Locale.ROOT,
                "events/year=%04d/month=%02d/day=%02d/events-%s.parquet",
                lb.getYear(), lb.getMonthValue(), lb.getDayOfMonth(), partition.tableName());
    }

    private static String quoteIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
