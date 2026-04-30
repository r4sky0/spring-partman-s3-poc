package com.example.archive.archive;

import com.example.archive.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;
import java.sql.Connection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies advisory-lock semantics against a real Postgres (Testcontainers, not a mock):
 * one session at a time can hold the lock, and a release lets the next caller acquire it.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class AdvisoryLockIntegrationTest {

    @Autowired
    DataSource dataSource;

    @Test
    void onlyOneSessionHoldsTheLockAtATime() throws Exception {
        long key = 0xDEADBEEFL;
        try (Connection a = dataSource.getConnection();
             Connection b = dataSource.getConnection()) {
            assertThat(AdvisoryLock.tryLock(a, key)).isTrue();
            // b draws a different physical connection from the pool, so this is a
            // distinct Postgres session and tryLock should fail until a releases.
            assertThat(AdvisoryLock.tryLock(b, key)).isFalse();

            assertThat(AdvisoryLock.unlock(a, key)).isTrue();
            assertThat(AdvisoryLock.tryLock(b, key)).isTrue();
            assertThat(AdvisoryLock.unlock(b, key)).isTrue();
        }
    }

    @Test
    void unlockReturnsFalseWhenCallerNeverHeldTheLock() throws Exception {
        long key = 0xCAFEBABEL;
        try (Connection c = dataSource.getConnection()) {
            // Postgres returns false (with a NOTICE) rather than raising — treat it
            // as a recoverable signal in callers, not a hard error.
            assertThat(AdvisoryLock.unlock(c, key)).isFalse();
        }
    }
}
