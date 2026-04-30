package com.example.archive.events;

import com.example.archive.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the JPA mapping end-to-end against a real Postgres container.
 * {@code @DataJpaTest} would swap in an in-memory DB and bypass partitioning,
 * so we boot the full Spring context with {@link TestcontainersConfiguration}.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class EventRepositoryTest {

    @Autowired
    EventRepository repository;

    @Autowired
    EventService service;

    @Test
    @Transactional
    void persistsAndReadsBackEventThroughJpa() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).withNano(0);
        // Ensure a partition exists for "now" before the JPA insert; pg_partman premake covers
        // the future, but a CI runner racing tests against midnight could be on the boundary.
        service.seed(1, 0);

        UUID tenant = UUID.randomUUID();
        EventEntity saved = repository.save(new EventEntity(
                tenant,
                "test.repository",
                "{\"k\":\"v\"}",
                now
        ));

        assertThat(saved.getId()).isNotNull();

        List<EventEntity> all = repository.findAll();
        assertThat(all)
                .filteredOn(e -> tenant.equals(e.getTenantId()))
                .singleElement()
                .satisfies(e -> {
                    assertThat(e.getEventType()).isEqualTo("test.repository");
                    assertThat(e.getPayloadJson()).contains("\"k\"").contains("\"v\"");
                    // OffsetDateTime survives the round-trip aligned to UTC (Hikari pins TIME ZONE,
                    // hibernate.jdbc.time_zone reinforces it on the driver side).
                    assertThat(e.getCreatedAt().toInstant()).isEqualTo(now.toInstant());
                });
    }
}
