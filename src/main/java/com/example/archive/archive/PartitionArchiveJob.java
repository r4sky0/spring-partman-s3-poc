package com.example.archive.archive;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

/**
 * Periodically (and on-demand via /archive/run) walks pg_partman's children for
 * the events parent, exports anything older than the retention horizon to S3,
 * then DETACHes and DROPs each archived partition. The S3 upload runs OUTSIDE
 * the DB transaction; the bookkeeping (archive_log insert + DETACH + DROP) runs
 * inside it. A failure between upload and commit just leaves an orphan S3 object
 * that the next tick re-PUTs to the same key (S3 PUT is idempotent on key).
 */
@Component
public class PartitionArchiveJob {

    private static final Logger log = LoggerFactory.getLogger(PartitionArchiveJob.class);

    private static final String INSERT_LOG_SQL = """
            INSERT INTO archive_log
                (partition_name, s3_bucket, s3_key, row_count, bytes_uploaded)
            VALUES (:name, :bucket, :key, :rows, :bytes)
            """;

    private final JdbcClient jdbcClient;
    private final PartitionDiscoveryService discovery;
    private final PartitionArchiver archiver;
    private final TransactionTemplate tx;
    private final Counter partitionsArchived;
    private final Counter rowsArchived;
    private final Counter bytesUploaded;
    private final Counter failures;

    public PartitionArchiveJob(
            JdbcClient jdbcClient,
            PartitionDiscoveryService discovery,
            PartitionArchiver archiver,
            TransactionTemplate tx,
            MeterRegistry meterRegistry) {
        this.jdbcClient = jdbcClient;
        this.discovery = discovery;
        this.archiver = archiver;
        this.tx = tx;
        this.partitionsArchived = meterRegistry.counter("archive.partitions.archived");
        this.rowsArchived = meterRegistry.counter("archive.rows.archived");
        // archive.bytes.uploaded counts S3 PUT bytes — a partition whose tx fails
        // and gets retried next cycle will contribute its size twice. Splitting
        // upload-vs-committed counters is left for production hardening.
        this.bytesUploaded = meterRegistry.counter("archive.bytes.uploaded");
        this.failures = meterRegistry.counter("archive.failures");
    }

    @Scheduled(fixedDelayString = "${archive.interval:60s}", initialDelayString = "${archive.interval:60s}")
    public void scheduledRun() {
        try {
            runOnce();
        } catch (Exception e) {
            log.error("Scheduled archive run failed", e);
        }
    }

    /**
     * Single archive cycle. Returns a summary suitable for HTTP response or test assertion.
     * Runs pg_partman maintenance first so any newly-eligible partitions exist before discovery.
     */
    public ArchiveSummary runOnce() {
        runMaintenance();
        List<PartitionInfo> candidates = discovery.findArchivable();
        log.info("Archive cycle: {} candidate partition(s) eligible", candidates.size());

        int archivedCount = 0;
        long totalRows = 0;
        long totalBytes = 0;
        for (PartitionInfo p : candidates) {
            try {
                ArchiveResult result = archiver.archive(p);
                tx.executeWithoutResult(_ -> {
                    jdbcClient.sql(INSERT_LOG_SQL)
                            .param("name", p.tableName())
                            .param("bucket", result.s3Bucket())
                            .param("key", result.s3Key())
                            .param("rows", result.rowCount())
                            .param("bytes", result.bytesUploaded())
                            .update();
                    detachAndDrop(p.tableName());
                });
                partitionsArchived.increment();
                rowsArchived.increment(result.rowCount());
                bytesUploaded.increment(result.bytesUploaded());
                archivedCount++;
                totalRows += result.rowCount();
                totalBytes += result.bytesUploaded();
                log.info("Archived partition {} → s3://{}/{}",
                        p.tableName(), result.s3Bucket(), result.s3Key());
            } catch (Exception e) {
                // Per-partition firebreak: SDK/JDBC/transaction failures stay scoped to one
                // partition. The cycle continues; the failed partition is rediscovered next
                // tick (no archive_log row → still a candidate) and the S3 PUT idempotency
                // makes the retry safe.
                failures.increment();
                log.error("Failed to archive partition {} — will retry next cycle", p.tableName(), e);
            }
        }
        return new ArchiveSummary(archivedCount, totalRows, totalBytes);
    }

    private void runMaintenance() {
        try {
            jdbcClient.sql("CALL partman.run_maintenance_proc()").update();
        } catch (DataAccessException e) {
            log.warn("partman.run_maintenance_proc() raised: {}", e.getMessage());
        }
    }

    /**
     * Non-concurrent DETACH takes ACCESS EXCLUSIVE on the parent for the duration of the
     * surrounding transaction. Production deployments should switch to {@code DETACH
     * PARTITION ... CONCURRENTLY} (Postgres 14+), which must run outside a transaction
     * block — a real implementation would split this into upload-then-detach phases.
     */
    private void detachAndDrop(String partitionName) {
        // Identifier interpolation is safe: name comes from pg_class.relname, not user input.
        String quoted = "\"" + partitionName.replace("\"", "\"\"") + "\"";
        jdbcClient.sql("ALTER TABLE events DETACH PARTITION " + quoted).update();
        jdbcClient.sql("DROP TABLE " + quoted).update();
    }

    public record ArchiveSummary(int partitionsArchived, long rowsArchived, long bytesUploaded) {}
}
