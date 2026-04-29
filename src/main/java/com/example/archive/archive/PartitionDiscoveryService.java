package com.example.archive.archive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
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

    /** Postgres TIMESTAMPTZ literals can come in several precisions; accept fractional seconds optionally. */
    private static final DateTimeFormatter PG_TS = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
            .appendOffset("+HH:mm", "+00")
            .toFormatter();

    private final JdbcClient jdbcClient;
    private final ArchiveProperties props;
    private final Clock clock;

    public PartitionDiscoveryService(JdbcClient jdbcClient, ArchiveProperties props, Clock clock) {
        this.jdbcClient = jdbcClient;
        this.props = props;
        this.clock = clock;
    }

    public List<PartitionInfo> findArchivable() {
        Instant cutoff = clock.instant().minus(props.retention());
        return jdbcClient.sql(CANDIDATE_SQL)
                .query((rs, rowNum) -> parsePartition(rs.getString("relname"), rs.getString("bound_expr")))
                .list()
                .stream()
                .filter(p -> p != null && !p.upperBound().toInstant().isAfter(cutoff))
                .toList();
    }

    private PartitionInfo parsePartition(String name, String boundExpr) {
        if (boundExpr == null) {
            log.warn("Partition {} has no bound expression — skipping", name);
            return null;
        }
        Matcher m = BOUND_PATTERN.matcher(boundExpr);
        if (!m.find()) {
            log.warn("Partition {} bound did not match expected RANGE format: {}", name, boundExpr);
            return null;
        }
        OffsetDateTime lower = parseTimestamp(m.group(1));
        OffsetDateTime upper = parseTimestamp(m.group(2));
        return new PartitionInfo(name, lower, upper);
    }

    private OffsetDateTime parseTimestamp(String raw) {
        try {
            return OffsetDateTime.parse(raw, PG_TS);
        } catch (Exception e) {
            return OffsetDateTime.parse(raw + "+00", PG_TS).withOffsetSameInstant(ZoneOffset.UTC);
        }
    }
}
