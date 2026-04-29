package com.example.archive.archive;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds child partitions of a parent table whose upper bound is older than the
 * configured retention horizon and that have not yet been recorded in
 * archive_log. Bounds are read from {@code pg_get_expr(relpartbound, oid)} —
 * the canonical Postgres source — rather than parsed from partition names.
 */
@Service
public class PartitionDiscoveryService {

    private static final Logger log = LoggerFactory.getLogger(PartitionDiscoveryService.class);

    /**
     * Children of public.events (excluding the partman default partition) along with
     * the textual bound expression Postgres stores in pg_class.relpartbound.
     */
    private static final String CANDIDATE_SQL = """
            SELECT c.relname,
                   pg_get_expr(c.relpartbound, c.oid) AS bound_expr
              FROM pg_inherits i
              JOIN pg_class p ON p.oid = i.inhparent
              JOIN pg_class c ON c.oid = i.inhrelid
             WHERE p.relname = 'events'
               AND p.relnamespace = 'public'::regnamespace
               AND c.relname NOT LIKE '%_default'
               AND c.relname NOT IN (SELECT partition_name FROM archive_log)
             ORDER BY c.relname
            """;

    /** Matches: FOR VALUES FROM ('2026-04-15 00:00:00+00') TO ('2026-04-16 00:00:00+00') */
    private static final Pattern BOUND_PATTERN = Pattern.compile(
            "FOR VALUES FROM \\('([^']+)'\\) TO \\('([^']+)'\\)");

    /**
     * Splits a Postgres TIMESTAMPTZ literal into datetime + offset. Postgres has been
     * observed to render the offset as {@code +00}, {@code +00:00}, {@code +02:30},
     * or {@code Z} depending on version/config — {@link ZoneOffset#of(String)} accepts
     * all of those forms, so doing the split-then-delegate is more robust than trying
     * to encode every shape into a single {@link DateTimeFormatter} pattern.
     */
    private static final Pattern PG_TS_PATTERN = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?)([+-]\\d.*|Z)$");

    /** Postgres TIMESTAMPTZ datetime portion; fractional seconds are optional. */
    private static final DateTimeFormatter PG_LOCAL = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
            .toFormatter();

    private final JdbcClient jdbcClient;
    private final ArchiveProperties props;
    private final Clock clock;
    private final Counter parseSkipped;

    public PartitionDiscoveryService(JdbcClient jdbcClient, ArchiveProperties props,
                                     Clock clock, MeterRegistry meterRegistry) {
        this.jdbcClient = jdbcClient;
        this.props = props;
        this.clock = clock;
        // archive.discovery.skipped: increments whenever a child partition's
        // pg_get_expr output can't be parsed into a (lower, upper) bound.
        // This is the early-warning signal for "Postgres started rendering
        // bounds in a shape we don't recognise" — without it, unparseable
        // partitions would silently grow the parent table forever.
        this.parseSkipped = meterRegistry.counter("archive.discovery.skipped");
    }

    public List<PartitionInfo> findArchivable() {
        Instant cutoff = clock.instant().minus(props.retention());
        return jdbcClient.sql(CANDIDATE_SQL)
                .query((rs, _) -> parsePartition(rs.getString("relname"), rs.getString("bound_expr")))
                .list()
                .stream()
                .filter(p -> p != null && !p.upperBound().toInstant().isAfter(cutoff))
                .toList();
    }

    /** Package-private for direct unit testing of the bound-string parser. */
    PartitionInfo parsePartition(String name, String boundExpr) {
        if (boundExpr == null) {
            log.warn("Partition {} has no bound expression — skipping", name);
            parseSkipped.increment();
            return null;
        }
        Matcher m = BOUND_PATTERN.matcher(boundExpr);
        if (!m.find()) {
            log.warn("Partition {} bound did not match expected RANGE format: {}", name, boundExpr);
            parseSkipped.increment();
            return null;
        }
        try {
            OffsetDateTime lower = parseTimestamp(m.group(1));
            OffsetDateTime upper = parseTimestamp(m.group(2));
            return new PartitionInfo(name, lower, upper);
        } catch (DateTimeParseException e) {
            log.warn("Partition {} bound failed to parse: {} ({})", name, boundExpr, e.getMessage());
            parseSkipped.increment();
            return null;
        }
    }

    private OffsetDateTime parseTimestamp(String raw) {
        Matcher m = PG_TS_PATTERN.matcher(raw);
        if (!m.matches()) {
            throw new DateTimeParseException("unrecognised TIMESTAMPTZ form", raw, 0);
        }
        LocalDateTime ldt = LocalDateTime.parse(m.group(1), PG_LOCAL);
        ZoneOffset offset = ZoneOffset.of(m.group(2));
        return OffsetDateTime.of(ldt, offset);
    }
}
