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
import org.springframework.test.context.TestPropertySource;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the missing-bucket failure path. Uses a {@code @TestPropertySource}
 * override so the lifecycle test's {@code archive-bucket} stays untouched, while this
 * context targets a bucket that is never created in LocalStack.
 *
 * <p>The archiver issues PutObject; LocalStack returns NoSuchBucket; the SDK raises
 * {@code S3Exception} which {@link PartitionArchiveJob#runOnce()} catches in its
 * per-partition firebreak. Expected effects: zero rows in {@code archive_log}, zero
 * archived in the summary, partition still attached.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "archive.bucket=archive-missing-bucket-test")
class ArchiveMissingBucketIntegrationTest {

    private static final String PARTITION_PREFIX = "events_pfailmissing_";

    @Autowired JdbcClient jdbc;
    @Autowired S3Client s3;
    @Autowired EventService events;
    @Autowired PartitionArchiveJob archiveJob;
    @Autowired ArchiveProperties archiveProps;

    @BeforeEach
    @AfterEach
    void cleanState() {
        // The bucket is intentionally NEVER created in LocalStack.
        cleanupScenarioState();
    }

    @Test
    void missingBucketLeavesArchiveLogCleanAndPartitionAttached() {
        // Sanity: this test owns a bucket that does not exist in LocalStack.
        assertThat(archiveProps.bucket()).isEqualTo("archive-missing-bucket-test");

        OffsetDateTime dayStart = OffsetDateTime.now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.DAYS).minusDays(35);
        String partition = createBackdatedPartition("nobucket", dayStart);
        for (int i = 0; i < 3; i++) {
            events.insert(new Event(UUID.randomUUID(), "failmissing." + i,
                    "{\"i\":" + i + "}", dayStart.plusMinutes(i)));
        }

        ArchiveSummary summary = archiveJob.runOnce();

        // PartitionArchiveJob's per-partition firebreak swallows the SDK exception
        // and increments the failures counter; nothing is detached or logged.
        assertThat(summary.partitionsArchived()).isZero();
        assertThat(summary.rowsArchived()).isZero();

        Integer logRows = jdbc.sql(
                "SELECT count(*)::int FROM archive_log WHERE partition_name = :name")
                .param("name", partition)
                .query(Integer.class).single();
        assertThat(logRows).isZero();

        Integer stillAttached = jdbc.sql("""
                SELECT count(*)::int FROM pg_inherits i
                  JOIN pg_class p ON p.oid = i.inhparent
                  JOIN pg_class c ON c.oid = i.inhrelid
                 WHERE p.relname = 'events' AND c.relname = :name
                """)
                .param("name", partition)
                .query(Integer.class).single();
        assertThat(stillAttached).as("failed partition must remain attached for retry").isEqualTo(1);
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
    }
}
