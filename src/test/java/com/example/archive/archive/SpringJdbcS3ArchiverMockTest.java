package com.example.archive.archive;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The "missing middle" of the test pyramid: exercises {@link SpringJdbcS3Archiver}
 * end-to-end through real PostgreSQL COPY + gzip framing while mocking
 * {@link S3Client}. No LocalStack, no Spring context — just a singleton vanilla
 * Postgres container shared across the JVM.
 *
 * <p>Captures the {@code putObject(Consumer, RequestBody)} call so we can assert
 * on the exact bucket/key/content-type the archiver constructs and on the gzip
 * body bytes themselves (magic header + gunzipped CSV row count).
 */
class SpringJdbcS3ArchiverMockTest {

    /**
     * JVM-singleton container. {@code .withReuse(true)} attaches to an already-running
     * container on subsequent {@code mvn test} runs (when the user opts in via
     * {@code ~/.testcontainers.properties}). Vanilla {@code postgres:16} is fine here
     * — this test only needs COPY, not pg_partman.
     */
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16").withReuse(true);

    static {
        POSTGRES.start();
    }

    private static final ArchiveProperties PROPS =
            new ArchiveProperties(Duration.ofDays(7), "test-bucket", Duration.ofHours(1));

    private DataSource dataSource;

    @BeforeAll
    static void resetSchema() throws Exception {
        try (Connection c = directConnection(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS archiver_mock_test");
            s.execute("CREATE TABLE archiver_mock_test (id int PRIMARY KEY, payload text)");
        }
    }

    private static Connection directConnection() throws Exception {
        var ds = new PGSimpleDataSource();
        ds.setURL(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        return ds.getConnection();
    }

    private DataSource dataSource() {
        if (dataSource == null) {
            var ds = new PGSimpleDataSource();
            ds.setURL(POSTGRES.getJdbcUrl());
            ds.setUser(POSTGRES.getUsername());
            ds.setPassword(POSTGRES.getPassword());
            dataSource = ds;
        }
        return dataSource;
    }

    @Test
    void putsHiveKeyedGzippedCsvWithExpectedRowCount() throws Exception {
        seedRows(3);
        S3Client s3 = mock(S3Client.class);
        SpringJdbcS3Archiver archiver = new SpringJdbcS3Archiver(dataSource(), s3, PROPS);

        PartitionInfo partition = new PartitionInfo(
                "archiver_mock_test",
                OffsetDateTime.of(2026, 4, 15, 0, 0, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2026, 4, 16, 0, 0, 0, 0, ZoneOffset.UTC));

        ArchiveResult result = archiver.archive(partition);

        @SuppressWarnings({"unchecked", "rawtypes"})
        ArgumentCaptor<Consumer<PutObjectRequest.Builder>> consumerCaptor =
                (ArgumentCaptor<Consumer<PutObjectRequest.Builder>>) (ArgumentCaptor) forClass(Consumer.class);
        ArgumentCaptor<RequestBody> bodyCaptor = forClass(RequestBody.class);
        verify(s3).putObject(consumerCaptor.capture(), bodyCaptor.capture());

        PutObjectRequest req = PutObjectRequest.builder()
                .applyMutation(consumerCaptor.getValue())
                .build();
        assertThat(req.bucket()).isEqualTo("test-bucket");
        assertThat(req.key()).isEqualTo(
                "events/year=2026/month=04/day=15/events-archiver_mock_test.csv.gz");
        assertThat(req.contentType()).isEqualTo("application/gzip");

        byte[] bytes;
        try (InputStream is = bodyCaptor.getValue().contentStreamProvider().newStream()) {
            bytes = is.readAllBytes();
        }
        // Gzip magic — anything else means we shipped raw or zstd-compressed bytes.
        assertThat(bytes[0]).isEqualTo((byte) 0x1f);
        assertThat(bytes[1]).isEqualTo((byte) 0x8b);

        List<String> csvLines = gunzipLines(bytes);
        assertThat(csvLines).hasSize(4);                  // header + 3 data rows
        assertThat(csvLines.get(0)).isEqualTo("id,payload");
        assertThat(csvLines.subList(1, 4))
                .containsExactlyInAnyOrder("0,row-0", "1,row-1", "2,row-2");

        assertThat(result.s3Bucket()).isEqualTo("test-bucket");
        assertThat(result.s3Key()).isEqualTo(req.key());
        assertThat(result.rowCount()).isEqualTo(3);
        assertThat(result.bytesUploaded()).isEqualTo(bytes.length);
    }

    @Test
    void emptyPartitionStillUploadsHeaderOnlyGzip() throws Exception {
        truncate();
        S3Client s3 = mock(S3Client.class);
        SpringJdbcS3Archiver archiver = new SpringJdbcS3Archiver(dataSource(), s3, PROPS);

        PartitionInfo partition = new PartitionInfo(
                "archiver_mock_test",
                OffsetDateTime.of(2026, 4, 15, 0, 0, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2026, 4, 16, 0, 0, 0, 0, ZoneOffset.UTC));

        ArchiveResult result = archiver.archive(partition);

        ArgumentCaptor<RequestBody> bodyCaptor = forClass(RequestBody.class);
        verify(s3).putObject(any(Consumer.class), bodyCaptor.capture());

        byte[] bytes;
        try (InputStream is = bodyCaptor.getValue().contentStreamProvider().newStream()) {
            bytes = is.readAllBytes();
        }
        List<String> csvLines = gunzipLines(bytes);
        assertThat(csvLines).containsExactly("id,payload"); // header only, zero data rows
        assertThat(result.rowCount()).isZero();
    }

    @Test
    void s3PutFailurePropagatesAsArchiveException() throws Exception {
        seedRows(1);
        S3Client s3 = mock(S3Client.class);
        when(s3.putObject(any(Consumer.class), any(RequestBody.class)))
                .thenThrow(S3Exception.builder()
                        .statusCode(503)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("SlowDown").build())
                        .message("simulated 5xx")
                        .build());

        SpringJdbcS3Archiver archiver = new SpringJdbcS3Archiver(dataSource(), s3, PROPS);
        PartitionInfo partition = new PartitionInfo(
                "archiver_mock_test",
                OffsetDateTime.of(2026, 4, 15, 0, 0, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2026, 4, 16, 0, 0, 0, 0, ZoneOffset.UTC));

        // The archiver itself does no retry/wrap on S3 errors — the SDK exception
        // surfaces directly. PartitionArchiveJob's per-partition firebreak handles it.
        assertThatThrownBy(() -> archiver.archive(partition))
                .isInstanceOf(S3Exception.class)
                .hasMessageContaining("simulated 5xx");
    }

    private void seedRows(int n) throws Exception {
        truncate();
        try (Connection c = directConnection(); Statement s = c.createStatement()) {
            for (int i = 0; i < n; i++) {
                s.execute("INSERT INTO archiver_mock_test (id, payload) VALUES (" + i + ", 'row-" + i + "')");
            }
        }
    }

    private void truncate() throws Exception {
        try (Connection c = directConnection(); Statement s = c.createStatement()) {
            s.execute("TRUNCATE archiver_mock_test");
        }
    }

    private static List<String> gunzipLines(byte[] gzipped) throws Exception {
        try (var in = new GZIPInputStream(new ByteArrayInputStream(gzipped));
             var br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return br.lines().toList();
        }
    }
}
