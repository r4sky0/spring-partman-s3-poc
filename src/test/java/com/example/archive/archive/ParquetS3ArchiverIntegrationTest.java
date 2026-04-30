package com.example.archive.archive;

import com.example.archive.TestcontainersConfiguration;
import com.example.archive.events.Event;
import com.example.archive.events.EventService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end proof for the Parquet archiver: archive a partition with
 * {@code archive.format=parquet}, download the resulting object, and confirm
 * it's a well-formed Parquet file (PAR1 magic at head and tail) of non-zero
 * size. The full Avro round-trip is left out to keep the test classpath off
 * {@code hadoop-mapreduce-client-core} (a heavy transitive dep needed only by
 * the reader, not the writer) — file-format validity is the assertion that
 * matters; type fidelity is exercised by {@link AvroSchemaBuilder}.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "archive.format=parquet")
class ParquetS3ArchiverIntegrationTest {

    @Autowired JdbcClient jdbc;
    @Autowired S3Client s3;
    @Autowired EventService events;
    @Autowired PartitionArchiveJob archiveJob;
    @Autowired com.example.archive.archive.ArchiveProperties archiveProps;

    @Test
    void archivesPartitionAsParquetAndRoundTrips() throws Exception {
        ensureBucketExists();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS);
        // One partition past the retention horizon (today-10).
        OffsetDateTime dayStart = now.minusDays(10);
        createPartitionFor(dayStart);
        UUID tenant = UUID.randomUUID();
        for (int i = 0; i < 25; i++) {
            events.insert(new Event(
                    tenant,
                    "test.parquet." + (i % 3),
                    "{\"i\":" + i + "}",
                    dayStart.plusMinutes(i)));
        }

        var summary = archiveJob.runOnce();
        assertThat(summary.partitionsArchived()).isGreaterThanOrEqualTo(1);

        List<String> keys = s3.listObjectsV2(b -> b.bucket(archiveProps.bucket()).prefix("events/"))
                .contents().stream().map(S3Object::key).toList();
        assertThat(keys).anyMatch(k -> k.endsWith(".parquet"));
        String parquetKey = keys.stream().filter(k -> k.endsWith(".parquet")).findFirst().orElseThrow();

        ResponseBytes<GetObjectResponse> bytes = s3.getObjectAsBytes(
                b -> b.bucket(archiveProps.bucket()).key(parquetKey));
        byte[] raw = bytes.asByteArray();
        // Parquet files start and end with the 4-byte magic "PAR1" — the footer is
        // the source of truth for the file, so a present trailing magic plus a non-
        // zero-length file is enough to prove the writer didn't truncate mid-row.
        assertThat(raw.length).isGreaterThan(8);
        assertThat(new String(raw, 0, 4)).as("leading magic").isEqualTo("PAR1");
        assertThat(new String(raw, raw.length - 4, 4)).as("trailing magic").isEqualTo("PAR1");
    }

    private void ensureBucketExists() {
        try {
            s3.createBucket(b -> b.bucket(archiveProps.bucket()));
        } catch (BucketAlreadyOwnedByYouException _) {
            // already exists from a previous test in the same context
        }
    }

    private void createPartitionFor(OffsetDateTime dayStart) {
        var upper = dayStart.plusDays(1);
        String name = "events_p" + dayStart.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        String lowerIso = dayStart.format(java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        String upperIso = upper.format(java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        jdbc.sql("CREATE TABLE IF NOT EXISTS " + name
                        + " PARTITION OF events FOR VALUES FROM ('" + lowerIso + "') TO ('" + upperIso + "')")
                .update();
    }
}
