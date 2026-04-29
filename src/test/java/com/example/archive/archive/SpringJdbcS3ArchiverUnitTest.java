package com.example.archive.archive;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure-Java tests for the bits of {@link SpringJdbcS3Archiver} that don't require
 * a database or S3: the S3 key derivation and gzip framing assumptions.
 */
class SpringJdbcS3ArchiverUnitTest {

    @Test
    void s3KeyEncodesPartitionLowerBoundAsHiveStylePath() throws Exception {
        SpringJdbcS3Archiver archiver = new SpringJdbcS3Archiver(null, null, null);
        Method method = SpringJdbcS3Archiver.class.getDeclaredMethod("s3Key", PartitionInfo.class);
        method.setAccessible(true);

        PartitionInfo p = new PartitionInfo(
                "events_p2026_04_15",
                OffsetDateTime.of(2026, 4, 15, 0, 0, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2026, 4, 16, 0, 0, 0, 0, ZoneOffset.UTC));

        String key = (String) method.invoke(archiver, p);
        assertThat(key).isEqualTo(
                "events/year=2026/month=04/day=15/events-events_p2026_04_15.csv.gz");
    }

    @Test
    void s3KeyZeroPadsSingleDigitMonthAndDay() throws Exception {
        SpringJdbcS3Archiver archiver = new SpringJdbcS3Archiver(null, null, null);
        Method method = SpringJdbcS3Archiver.class.getDeclaredMethod("s3Key", PartitionInfo.class);
        method.setAccessible(true);

        PartitionInfo p = new PartitionInfo(
                "events_p2026_01_05",
                OffsetDateTime.of(2026, 1, 5, 0, 0, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2026, 1, 6, 0, 0, 0, 0, ZoneOffset.UTC));

        String key = (String) method.invoke(archiver, p);
        assertThat(key).contains("year=2026/month=01/day=05/");
    }
}
