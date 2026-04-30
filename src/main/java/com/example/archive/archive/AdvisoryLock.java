package com.example.archive.archive;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Thin wrapper around Postgres advisory locks. Used by the archive job to gate
 * a single cycle to one instance at a time — without this, two replicas would
 * race on {@code DETACH PARTITION} (which takes ACCESS EXCLUSIVE on the parent).
 *
 * <h2>Why session-scoped, not transaction-scoped</h2>
 * {@code runOnce()} spans many transactions: pg_partman maintenance, per-partition
 * DETACH/DROP transactions, and S3 IO between them. A {@code pg_advisory_xact_lock}
 * would release at every commit. Session-scoped locks (the {@code pg_*advisory_lock}
 * pair without {@code _xact_}) survive transaction boundaries.
 *
 * <h2>Why a dedicated connection</h2>
 * Session-scoped advisory locks survive a connection's return to the pool —
 * HikariCP does not reset them. Acquiring the lock on a pooled connection
 * (e.g. through a {@code JdbcClient} call) would mean the lock outlives the
 * statement and could be released later by a completely unrelated caller that
 * happened to draw the same physical connection. Callers therefore manage a
 * dedicated {@link Connection} taken straight from the {@code DataSource}.
 */
public final class AdvisoryLock {

    /** Stable arbitrary key for the events-archive job. Different jobs should use different keys. */
    public static final long ARCHIVE_EVENTS_KEY = 0xA5C81EACL;

    private AdvisoryLock() {}

    /** Returns true if the lock was acquired, false if another session holds it. */
    public static boolean tryLock(Connection conn, long key) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            ps.setLong(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    /**
     * Releases a lock previously acquired by this same session. Returns true if
     * the lock was actually released (Postgres also returns false if the caller
     * never held it; treat false as a recoverable signal, not an error).
     */
    public static boolean unlock(Connection conn, long key) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT pg_advisory_unlock(?)")) {
            ps.setLong(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }
}
