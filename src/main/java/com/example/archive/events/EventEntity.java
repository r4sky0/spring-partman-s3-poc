package com.example.archive.events;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * JPA mapping for the {@code events} parent table.
 *
 * <h2>Partitioning contract</h2>
 * The underlying table is range-partitioned daily by {@code created_at} and
 * managed by pg_partman ({@code V1__events_table.sql}, {@code V2__partman_config.sql}).
 * The composite PK {@code (id, created_at)} is required by Postgres native
 * partitioning — the partition key must be part of the PK — and is mirrored
 * here via {@link IdClass} on {@link EventId}.
 *
 * <h2>Lifecycle</h2>
 * Treat this entity as <strong>append-only</strong>. The archive job
 * ({@code PartitionArchiveJob}) deletes data via {@code DROP PARTITION} after
 * exporting it to S3; never call {@code repository.delete(...)} or mutate a
 * persisted instance, or you risk fighting the archive lifecycle.
 *
 * <p>JSON payloads are stored in the Postgres {@code JSONB} column via Hibernate 6's
 * native {@code @JdbcTypeCode(SqlTypes.JSON)} mapping — no third-party hibernate-types
 * dependency required.
 */
@Entity
@Table(name = "events")
@IdClass(EventId.class)
public class EventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Id
    @Column(name = "created_at", updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "tenant_id", nullable = false, updatable = false)
    private UUID tenantId;

    @Column(name = "event_type", nullable = false, updatable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, updatable = false)
    private String payloadJson;

    public EventEntity() {}

    public EventEntity(UUID tenantId, String eventType, String payloadJson, OffsetDateTime createdAt) {
        this.tenantId = tenantId;
        this.eventType = eventType;
        this.payloadJson = payloadJson;
        this.createdAt = createdAt;
    }

    public Long getId() { return id; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
    public UUID getTenantId() { return tenantId; }
    public String getEventType() { return eventType; }
    public String getPayloadJson() { return payloadJson; }
}
