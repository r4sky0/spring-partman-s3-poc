package com.example.archive.archive;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure-Java tests for the bound-string parser in {@link PartitionDiscoveryService}.
 * These cover the shapes Postgres has been observed to render in
 * {@code pg_get_expr(c.relpartbound, c.oid)} across versions/configs.
 *
 * <p>The headline integration test only sees one bound shape (whatever the running
 * Postgres image produces). A version bump that nudges the rendering format would
 * silently move every partition to "skipped — unparseable" without these tests.
 */
class PartitionDiscoveryServiceTest {

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final PartitionDiscoveryService svc = new PartitionDiscoveryService(null, null, null, meters);

    @Test
    void parsesShortOffsetForm() {
        PartitionInfo info = svc.parsePartition("events_p2026_04_15",
                "FOR VALUES FROM ('2026-04-15 00:00:00+00') TO ('2026-04-16 00:00:00+00')");

        assertThat(info).isNotNull();
        assertThat(info.tableName()).isEqualTo("events_p2026_04_15");
        assertThat(info.lowerBound().toInstant())
                .isEqualTo(java.time.Instant.parse("2026-04-15T00:00:00Z"));
        assertThat(info.upperBound().toInstant())
                .isEqualTo(java.time.Instant.parse("2026-04-16T00:00:00Z"));
    }

    @Test
    void parsesFullOffsetForm() {
        // Some Postgres builds render the offset as +HH:mm rather than +HH.
        PartitionInfo info = svc.parsePartition("events_p2026_04_15",
                "FOR VALUES FROM ('2026-04-15 00:00:00+00:00') TO ('2026-04-16 00:00:00+00:00')");

        assertThat(info).isNotNull();
        assertThat(info.lowerBound().getOffset()).isEqualTo(ZoneOffset.UTC);
    }

    @Test
    void parsesFractionalSeconds() {
        PartitionInfo info = svc.parsePartition("events_p2026_04_15",
                "FOR VALUES FROM ('2026-04-15 00:00:00.123456+00') TO ('2026-04-16 00:00:00.987654+00')");

        assertThat(info).isNotNull();
        assertThat(info.lowerBound().getNano()).isEqualTo(123_456_000);
        assertThat(info.upperBound().getNano()).isEqualTo(987_654_000);
    }

    @Test
    void parsesNonUtcOffsetAndPreservesInstant() {
        // A misconfigured session might render in a non-UTC offset. The instant must still be correct.
        PartitionInfo info = svc.parsePartition("events_p2026_04_15",
                "FOR VALUES FROM ('2026-04-15 02:00:00+02:00') TO ('2026-04-16 02:00:00+02:00')");

        assertThat(info).isNotNull();
        assertThat(info.lowerBound().toInstant())
                .isEqualTo(java.time.Instant.parse("2026-04-15T00:00:00Z"));
    }

    @Test
    void returnsNullAndIncrementsSkippedForMalformedBound() {
        double before = meters.counter("archive.discovery.skipped").count();

        PartitionInfo info = svc.parsePartition("events_weird",
                "LIST ('foo')"); // not a RANGE bound at all

        assertThat(info).isNull();
        assertThat(meters.counter("archive.discovery.skipped").count()).isEqualTo(before + 1);
    }

    @Test
    void returnsNullAndIncrementsSkippedForNullBound() {
        double before = meters.counter("archive.discovery.skipped").count();

        PartitionInfo info = svc.parsePartition("events_no_bound", null);

        assertThat(info).isNull();
        assertThat(meters.counter("archive.discovery.skipped").count()).isEqualTo(before + 1);
    }
}
