package com.example.archive;

import com.example.archive.archive.ArchiveProperties;
import com.example.archive.archive.PartitionArchiveJob;
import com.example.archive.archive.PartitionArchiveJob.ArchiveSummary;
import com.example.archive.events.Event;
import com.example.archive.events.EventService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end proof that the archive lifecycle works:
 *   1. Daily partitions are created for the last 14 days.
 *   2. Each gets a known number of rows.
 *   3. POST /archive/run (well, archiveJob.runOnce()) is invoked.
 *   4. Exactly the partitions older than the 7-day retention window are uploaded
 *      to LocalStack S3 as gzipped CSV, recorded in archive_log, and dropped.
 *   5. A second run is a no-op.
 *
 * Bounds-by-day arithmetic with retention=P7D and {@code now} = today T:
 * a partition for day D (lower=D 00:00, upper=(D+1) 00:00) is archivable iff
 * (D+1) 00:00 <= T - 7D. Seeding D ∈ {today-14 … today-1} therefore yields exactly
 * 7 archivable partitions (today-14 … today-8) regardless of the time of day the
 * test runs.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ArchiveLifecycleIntegrationTest {

    private static final int ROWS_PER_DAY = 50;

    @Autowired JdbcClient jdbc;
    @Autowired S3Client s3;
    @Autowired EventService events;
    @Autowired PartitionArchiveJob archiveJob;
    @Autowired ArchiveProperties archiveProps;

    @Test
    void archivesOldPartitionsAndIsIdempotent() throws Exception {
        ensureBucketExists();
        seedFourteenDays();

        int childrenBefore = countChildPartitions();
        assertThat(childrenBefore).isGreaterThanOrEqualTo(14);

        // ---- First run: should archive exactly 7 partitions (today-14 … today-8) ----
        ArchiveSummary first = archiveJob.runOnce();
        assertThat(first.partitionsArchived()).isEqualTo(7);
        assertThat(first.rowsArchived()).isEqualTo(7L * ROWS_PER_DAY);

        List<String> keys = listArchiveKeys();
        assertThat(keys).hasSize(7);
        assertThat(keys).allMatch(k -> k.startsWith("events/year=") && k.endsWith(".csv.gz"));

        Integer logRows = jdbc.sql("SELECT count(*)::int FROM archive_log")
                .query(Integer.class).single();
        assertThat(logRows).isEqualTo(7);

        Integer childrenAfter = countChildPartitions();
        assertThat(childrenAfter).isEqualTo(childrenBefore - 7);

        // ---- The CSV.gz objects round-trip: each gunzipped file has 50 data rows + header ----
        for (String key : keys) {
            long dataRows = countDataRowsInGzCsv(key);
            assertThat(dataRows).as("row count in %s", key).isEqualTo(ROWS_PER_DAY);
        }

        // ---- Second run: nothing left to archive ----
        ArchiveSummary second = archiveJob.runOnce();
        assertThat(second.partitionsArchived()).isZero();
        assertThat(second.rowsArchived()).isZero();
    }

    private void ensureBucketExists() {
        try {
            s3.createBucket(b -> b.bucket(archiveProps.bucket()));
        } catch (BucketAlreadyOwnedByYouException _) {
            // already there from a previous test run within the same context
        }
    }

    /** Seeds today-14 … today-1 (14 daily partitions, each with ROWS_PER_DAY events). */
    private void seedFourteenDays() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS);
        for (int d = 1; d <= 14; d++) {
            OffsetDateTime dayStart = now.minusDays(d);
            createPartitionFor(dayStart);
            for (int i = 0; i < ROWS_PER_DAY; i++) {
                OffsetDateTime ts = dayStart.plusMinutes(i);
                events.insert(new Event(
                        UUID.randomUUID(),
                        "test." + (i % 3),
                        "{\"i\":" + i + "}",
                        ts
                ));
            }
        }
    }

    private void createPartitionFor(OffsetDateTime dayStart) {
        // Bypass partman.create_partition_time for backdated partitions: in v5 it refuses
        // to create partitions behind its current high-water mark. Raw DDL is fine because
        // the Postgres native partitioning engine doesn't care who created the child;
        // pg_partman picks it up on the next show_partitions() call via pg_inherits.
        var lower = dayStart;
        var upper = dayStart.plusDays(1);
        String name = "events_p" + dayStart.toLocalDate()
                .format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
        String lowerIso = lower.format(java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        String upperIso = upper.format(java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        jdbc.sql("CREATE TABLE IF NOT EXISTS " + name
                + " PARTITION OF events FOR VALUES FROM ('" + lowerIso + "') TO ('" + upperIso + "')")
                .update();
    }

    private int countChildPartitions() {
        return jdbc.sql("""
                SELECT count(*)::int FROM pg_inherits i
                  JOIN pg_class p ON p.oid = i.inhparent
                  JOIN pg_class c ON c.oid = i.inhrelid
                 WHERE p.relname = 'events'
                   AND c.relname NOT LIKE '%_default'
                """).query(Integer.class).single();
    }

    private List<String> listArchiveKeys() {
        return s3.listObjectsV2(b -> b.bucket(archiveProps.bucket()).prefix("events/"))
                .contents().stream()
                .map(S3Object::key)
                .sorted()
                .toList();
    }

    private long countDataRowsInGzCsv(String key) throws Exception {
        ResponseBytes<GetObjectResponse> bytes = s3.getObjectAsBytes(
                b -> b.bucket(archiveProps.bucket()).key(key));
        try (var gz = new GZIPInputStream(new ByteArrayInputStream(bytes.asByteArray()));
             var lnr = new LineNumberReader(new InputStreamReader(gz))) {
            long lines = 0;
            while (lnr.readLine() != null) lines++;
            return Math.max(0, lines - 1); // subtract HEADER row
        }
    }
}
