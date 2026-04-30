package com.example.archive.archive;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

/**
 * Exports a partition as Apache Parquet (columnar, compressed, directly
 * queryable by Athena/DuckDB/Spark from S3) instead of gzipped CSV. Selected
 * via {@code archive.format=parquet}.
 *
 * <p>Trades raw streaming for simplicity: the writer assembles the file on
 * local disk first, then hands it to the Transfer Manager. Disk I/O is
 * dwarfed by S3 upload time for any non-trivial partition, and writing to
 * disk avoids the extra producer-thread plumbing the streaming CSV path needs
 * just to make {@code AvroParquetWriter} happy.
 *
 * <h2>Type fidelity</h2>
 * Schema derivation is dynamic via {@link AvroSchemaBuilder#fromResultSet} so
 * adding columns to {@code events} doesn't require code changes. Mapping is
 * conservative — Postgres-specific types (UUID, JSONB) are stored as strings;
 * timestamps go through Avro's {@code timestamp-micros} logical type.
 */
@Service
@ConditionalOnProperty(name = "archive.format", havingValue = "parquet")
public class ParquetS3Archiver implements PartitionArchiver {

    private static final Logger log = LoggerFactory.getLogger(ParquetS3Archiver.class);

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
        String selectSql = "SELECT * FROM " + quoteIdent(partition.tableName());

        Path tempFile;
        try {
            tempFile = Files.createTempFile("partition-" + partition.tableName() + "-", ".parquet");
        } catch (IOException e) {
            throw new ArchiveException("Could not create temp file for " + partition.tableName(), e);
        }

        long rowCount;
        try {
            // Postgres has no native temp-file collision concern; our concern is whether
            // ParquetWriter can write to the path. AvroParquetWriter requires the file
            // not to exist (OVERWRITE not honored when the path is empty), so delete first.
            Files.deleteIfExists(tempFile);
            rowCount = writeParquet(selectSql, partition.tableName(), tempFile);
        } catch (Exception e) {
            deleteQuietly(tempFile);
            if (e instanceof ArchiveException ae) throw ae;
            throw new ArchiveException("Parquet write failed for " + partition.tableName(), e);
        }

        long bytesUploaded;
        try {
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

    private long writeParquet(String selectSql, String partitionName, Path tempFile) {
        // LocalOutputFile (parquet-common) sidesteps hadoop-common entirely: it writes
        // through java.nio.file directly, so we get Parquet output without dragging
        // in jersey, kerby, woodstox, commons-* and the rest of the Hadoop universe.
        LocalOutputFile out = new LocalOutputFile(tempFile);

        return jdbcTemplate.execute((java.sql.Connection conn) -> {
            try (var stmt = conn.prepareStatement(selectSql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                stmt.setFetchSize(1000);
                try (ResultSet rs = stmt.executeQuery()) {
                    Schema schema = AvroSchemaBuilder.fromResultSet(partitionName, rs.getMetaData());
                    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                            .<GenericRecord>builder(out)
                            .withSchema(schema)
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .build()) {
                        return streamRows(rs, schema, writer);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private long streamRows(ResultSet rs, Schema schema, ParquetWriter<GenericRecord> writer)
            throws SQLException, IOException {
        ResultSetMetaData md = rs.getMetaData();
        long rows = 0;
        while (rs.next()) {
            GenericRecord record = new GenericData.Record(schema);
            for (int i = 1; i <= md.getColumnCount(); i++) {
                record.put(md.getColumnLabel(i), readColumn(rs, i, md));
            }
            writer.write(record);
            rows++;
        }
        return rows;
    }

    private static Object readColumn(ResultSet rs, int i, ResultSetMetaData md) throws SQLException {
        int sqlType = md.getColumnType(i);
        Object value = switch (sqlType) {
            case Types.BIGINT -> {
                long v = rs.getLong(i);
                yield rs.wasNull() ? null : v;
            }
            case Types.INTEGER, Types.SMALLINT -> {
                int v = rs.getInt(i);
                yield rs.wasNull() ? null : v;
            }
            case Types.BOOLEAN, Types.BIT -> {
                boolean v = rs.getBoolean(i);
                yield rs.wasNull() ? null : v;
            }
            case Types.DOUBLE -> {
                double v = rs.getDouble(i);
                yield rs.wasNull() ? null : v;
            }
            case Types.FLOAT, Types.REAL -> {
                float v = rs.getFloat(i);
                yield rs.wasNull() ? null : v;
            }
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
                OffsetDateTime odt = rs.getObject(i, OffsetDateTime.class);
                yield odt == null ? null : ChronoUnit.MICROS.between(Instant.EPOCH, odt.toInstant());
            }
            case Types.DATE -> {
                java.sql.Date d = rs.getDate(i);
                yield d == null ? null : (int) d.toLocalDate().toEpochDay();
            }
            case Types.OTHER -> readOther(rs, i, md.getColumnTypeName(i));
            default -> rs.getString(i);
        };
        return value;
    }

    /**
     * Postgres-specific OTHER types: uuid and jsonb both come through as PGobject.
     * Stringify in both cases — analytics consumers re-parse JSON on read; UUID
     * round-trips cleanly as its canonical hyphenated form.
     */
    private static String readOther(ResultSet rs, int i, String typeName) throws SQLException {
        Object o = rs.getObject(i);
        if (o == null) return null;
        if (o instanceof UUID u) return u.toString();
        if (o instanceof PGobject pg) return pg.getValue();
        return o.toString();
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
