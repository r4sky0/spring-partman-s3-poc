package com.example.archive.archive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * Finds child partitions of {@code public.events} whose upper bound is older
 * than the configured retention horizon and that have not yet been recorded in
 * {@code archive_log}. Bounds come from {@code partman.show_partition_info()},
 * the documented pg_partman v5 API — no regex over {@code pg_get_expr}, no
 * timezone-rendering edge cases.
 */
@Service
public class PartitionDiscoveryService {

    private static final Logger log = LoggerFactory.getLogger(PartitionDiscoveryService.class);

    /**
     * Joins {@code partman.show_partitions} (the children, default excluded) with a
     * lateral {@code partman.show_partition_info} call (the start/end bounds for each
     * child). Anything already in {@code archive_log} is filtered out so a cycle
     * that finishes the upload but crashes before commit is safely re-picked.
     *
     * <p>{@code child_end_time} is non-inclusive — it matches the partition's
     * Postgres FOR-VALUES-TO upper bound, so the retention comparison stays simple
     * (a partition is archivable iff its upper bound has fully passed the cutoff).
     */
    private static final String CANDIDATE_SQL = """
            SELECT sp.partition_tablename AS relname,
                   info.child_start_time   AS lower_bound,
                   info.child_end_time     AS upper_bound
              FROM partman.show_partitions('public.events', 'ASC', false) sp,
                   LATERAL partman.show_partition_info(
                       sp.partition_schemaname || '.' || sp.partition_tablename,
                       NULL,
                       'public.events'
                   ) info
             WHERE sp.partition_tablename NOT IN (SELECT partition_name FROM archive_log)
             ORDER BY sp.partition_tablename
            """;

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
        List<PartitionInfo> all = jdbcClient.sql(CANDIDATE_SQL)
                .query((rs, _) -> new PartitionInfo(
                        rs.getString("relname"),
                        rs.getObject("lower_bound", OffsetDateTime.class),
                        rs.getObject("upper_bound", OffsetDateTime.class)))
                .list();

        List<PartitionInfo> archivable = all.stream()
                .filter(p -> !p.upperBound().toInstant().isAfter(cutoff))
                .toList();

        log.debug("Discovered {} child partition(s); {} archivable past cutoff {}",
                all.size(), archivable.size(), cutoff);
        return archivable;
    }
}
