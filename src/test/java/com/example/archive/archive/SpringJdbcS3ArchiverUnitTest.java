package com.example.archive.archive;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure-Java tests for the bits of {@link SpringJdbcS3Archiver} that don't require
 * a database or S3: the S3 key derivation. Direct calls to the package-private
 * static {@code s3Key} method — no reflection, no dummy archiver instance.
 */
class SpringJdbcS3ArchiverUnitTest {

    @Test
    void s3KeyEncodesPartitionLowerBoundAsHiveStylePath() {
        PartitionInfo p = new PartitionInfo(
                "events_p2026_04_15",
                OffsetDateTime.of(2026, 4, 15, 0, 0, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2026, 4, 16, 0, 0, 0, 0, ZoneOffset.UTC));

        assertThat(SpringJdbcS3Archiver.s3Key(p)).isEqualTo(
                "events/year=2026/month=04/day=15/events-events_p2026_04_15.csv.gz");
    }

    @Test
    void s3KeyZeroPadsSingleDigitMonthAndDay() {
        PartitionInfo p = new PartitionInfo(
                "events_p2026_01_05",
                OffsetDateTime.of(2026, 1, 5, 0, 0, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2026, 1, 6, 0, 0, 0, 0, ZoneOffset.UTC));

        assertThat(SpringJdbcS3Archiver.s3Key(p)).contains("year=2026/month=01/day=05/");
    }
}
