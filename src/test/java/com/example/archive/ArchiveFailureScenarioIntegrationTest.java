package com.example.archive;

import com.example.archive.archive.ArchiveProperties;
import com.example.archive.archive.PartitionArchiveJob;
import com.example.archive.archive.PartitionArchiveJob.ArchiveSummary;
import com.example.archive.events.Event;
import com.example.archive.events.EventService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Edge-case coverage for the archive job against LocalStack S3:
 * <ul>
 *   <li>Empty partition — produces a header-only gzip and still records archive_log.</li>
 *   <li>Pre-existing key — putObject overwrites; job records archive_log normally.</li>
 * </ul>
 *
 * <p>NoSuchBucket failure lives in {@link ArchiveMissingBucketIntegrationTest} so its
 * {@code archive.bucket} override doesn't pollute this context. Simulated 5xx is
 * exercised at the Mockito tier ({@code SpringJdbcS3ArchiverMockTest}) where it's
 * deterministic — LocalStack OSS doesn't reliably inject server errors.
 *
 * <p>All test partitions use the {@code events_pfailscen_*} naming prefix and live
 * 30+ days in the past so they don't collide with the daily partitions
 * {@link ArchiveLifecycleIntegrationTest} or pg_partman's premake range create.
 * Cleanup is defensive: drop any leftover prefix-matching child, delete its
 * archive_log row, and remove its S3 object — so subsequent test classes
 * (notably the lifecycle test that asserts {@code archive_log == 7}) see a
 * pristine slate even if a method fails partway through.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ArchiveFailureScenarioIntegrationTest {

    private static final String PARTITION_PREFIX = "events_pfailscen_";

    @Autowired JdbcClient jdbc;
    @Autowired S3Client s3;
    @Autowired EventService events;
    @Autowired PartitionArchiveJob archiveJob;
    @Autowired ArchiveProperties archiveProps;

    @BeforeEach
    void cleanState() {
        ensureBucketExists();
        cleanupScenarioState();
    }

    @AfterEach
    void afterEach() {
        cleanupScenarioState();
    }

    @Test
    void emptyPartitionUploadsHeaderOnlyGzipAndRecordsArchiveLog() throws Exception {
        OffsetDateTime dayStart = utcDay(30);
        String partition = createBackdatedPartition("empty", dayStart);

        ArchiveSummary summary = archiveJob.runOnce();

        assertThat(summary.partitionsArchived()).isEqualTo(1);
        assertThat(summary.rowsArchived()).isZero();

        Integer logRows = jdbc.sql(
                "SELECT count(*)::int FROM archive_log WHERE partition_name = :name")
                .param("name", partition)
                .query(Integer.class).single();
        assertThat(logRows).isEqualTo(1);

        String key = jdbc.sql("SELECT s3_key FROM archive_log WHERE partition_name = :name")
                .param("name", partition)
                .query(String.class).single();
        assertThat(key).startsWith("events/year=").endsWith(partition + ".csv.gz");

        long dataRows = countDataRowsInGzCsv(key);
        assertThat(dataRows).as("empty partition produces header-only csv.gz").isZero();
    }

    @Test
    void preExistingKeyIsOverwrittenByArchiver() throws Exception {
        OffsetDateTime dayStart = utcDay(31);
        String partition = createBackdatedPartition("prekey", dayStart);
        seedRows(dayStart, 5);

        // Same Hive layout the archiver will compute. We pre-populate this exact key
        // with garbage to prove the job overwrites rather than skips.
        String expectedKey = String.format(
                "events/year=%04d/month=%02d/day=%02d/events-%s.csv.gz",
                dayStart.getYear(), dayStart.getMonthValue(), dayStart.getDayOfMonth(), partition);
        s3.putObject(b -> b.bucket(archiveProps.bucket()).key(expectedKey),
                RequestBody.fromString("not a real gzip — sentinel"));

        ArchiveSummary summary = archiveJob.runOnce();
        assertThat(summary.partitionsArchived()).isEqualTo(1);
        assertThat(summary.rowsArchived()).isEqualTo(5);

        long dataRows = countDataRowsInGzCsv(expectedKey);
        assertThat(dataRows).as("sentinel overwritten with real csv.gz").isEqualTo(5);
    }

    private OffsetDateTime utcDay(int daysAgo) {
        return OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).minusDays(daysAgo);
    }

    private String createBackdatedPartition(String suffix, OffsetDateTime dayStart) {
        String name = PARTITION_PREFIX + suffix;
        String lowerIso = dayStart.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        String upperIso = dayStart.plusDays(1).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        jdbc.sql("CREATE TABLE IF NOT EXISTS " + name
                        + " PARTITION OF events FOR VALUES FROM ('" + lowerIso + "') TO ('" + upperIso + "')")
                .update();
        return name;
    }

    private void seedRows(OffsetDateTime dayStart, int n) {
        for (int i = 0; i < n; i++) {
            events.insert(new Event(
                    UUID.randomUUID(),
                    "failscen." + i,
                    "{\"i\":" + i + "}",
                    dayStart.plusMinutes(i)));
        }
    }

    private void ensureBucketExists() {
        try {
            s3.createBucket(b -> b.bucket(archiveProps.bucket()));
        } catch (BucketAlreadyOwnedByYouException _) {
            // already provisioned by another test in the shared LocalStack
        }
    }

    /**
     * Idempotent cleanup of every artefact a failure-scenario test could leave behind:
     * child partitions in the events tree, archive_log rows, and S3 objects whose
     * key contains the prefix. Always runs to defend against partial failure.
     */
    private void cleanupScenarioState() {
        List<String> stalePartitions = jdbc.sql("""
                SELECT c.relname FROM pg_inherits i
                  JOIN pg_class p ON p.oid = i.inhparent
                  JOIN pg_class c ON c.oid = i.inhrelid
                 WHERE p.relname = 'events'
                   AND c.relname LIKE :prefix
                """)
                .param("prefix", PARTITION_PREFIX + "%")
                .query(String.class).list();
        for (String name : stalePartitions) {
            String quoted = "\"" + name.replace("\"", "\"\"") + "\"";
            jdbc.sql("ALTER TABLE events DETACH PARTITION " + quoted).update();
            jdbc.sql("DROP TABLE " + quoted).update();
        }

        jdbc.sql("DELETE FROM archive_log WHERE partition_name LIKE :prefix")
                .param("prefix", PARTITION_PREFIX + "%")
                .update();

        s3.listObjectsV2(b -> b.bucket(archiveProps.bucket()).prefix("events/"))
                .contents().stream()
                .map(S3Object::key)
                .filter(k -> k.contains(PARTITION_PREFIX))
                .forEach(k -> s3.deleteObject(b -> b.bucket(archiveProps.bucket()).key(k)));
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
