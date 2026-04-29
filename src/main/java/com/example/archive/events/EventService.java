package com.example.archive.events;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class EventService {

    private static final String INSERT_SQL = """
            INSERT INTO events (tenant_id, event_type, payload, created_at)
            VALUES (:tenantId, :eventType, :payload::jsonb, :createdAt)
            """;

    /** Matches pg_partman v5's default daily-suffix format ({@code pYYYYMMDD}) so
     *  {@code IF NOT EXISTS} deduplicates against the (today ± premake) partitions
     *  pg_partman already created. {@code ofPattern} (rather than
     *  {@code BASIC_ISO_DATE}) omits the trailing offset on OffsetDateTime input. */
    private static final DateTimeFormatter PARTMAN_DAILY_SUFFIX =
            DateTimeFormatter.ofPattern("yyyyMMdd");

    private final JdbcClient jdbcClient;

    public EventService(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public void insert(Event event) {
        // No @Transactional: a single INSERT runs in its own implicit tx in autocommit
        // mode; bulk callers (seed, future batch endpoints) supply the outer @Transactional.
        jdbcClient.sql(INSERT_SQL)
                .param("tenantId", event.tenantId())
                .param("eventType", event.eventType())
                .param("payload", event.payloadJson())
                // OffsetDateTime preserves the UTC offset on the wire; java.sql.Timestamp
                // would render in the JVM-local timezone and shift partition assignment.
                .param("createdAt", event.createdAt())
                .update();
    }

    /**
     * Generates {@code perDay} synthetic events for each of the last {@code days} days
     * (inclusive of today). For every day, ensures a per-day child partition exists
     * before inserting — pg_partman's {@code premake} only fabricates current+future
     * partitions, so backdated days would otherwise route into {@code events_default}
     * and be invisible to the archive job.
     */
    @Transactional
    public int seed(int days, int perDay) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        int total = 0;
        for (int d = 0; d < days; d++) {
            OffsetDateTime dayStart = now.minusDays(d).withHour(0).withMinute(0).withSecond(0).withNano(0);
            ensurePartitionForDay(dayStart);
            for (int i = 0; i < perDay; i++) {
                long offsetMs = ThreadLocalRandom.current().nextLong(86_400_000L);
                OffsetDateTime ts = dayStart.plusNanos(offsetMs * 1_000_000L);
                insert(new Event(
                        UUID.randomUUID(),
                        "demo." + ThreadLocalRandom.current().nextInt(5),
                        "{\"i\":" + i + ",\"d\":" + d + "}",
                        ts
                ));
                total++;
            }
        }
        return total;
    }

    /**
     * Creates {@code events_pYYYYMMDD} as a range partition covering [dayStart, dayStart+1d).
     * Uses raw DDL because pg_partman v5's {@code create_partition_time} refuses to fabricate
     * children behind its current high-water mark. {@code IF NOT EXISTS} makes calls for the
     * (today ± premake) range a no-op against the partitions pg_partman already premade.
     */
    private void ensurePartitionForDay(OffsetDateTime dayStart) {
        String name = "events_p" + dayStart.format(PARTMAN_DAILY_SUFFIX);
        String lowerIso = dayStart.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        String upperIso = dayStart.plusDays(1).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        jdbcClient.sql("CREATE TABLE IF NOT EXISTS " + name
                        + " PARTITION OF events FOR VALUES FROM ('" + lowerIso + "') TO ('" + upperIso + "')")
                .update();
    }
}
