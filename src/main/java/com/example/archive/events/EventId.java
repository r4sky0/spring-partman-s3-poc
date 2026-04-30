package com.example.archive.events;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Composite primary-key holder for {@link EventEntity}. The {@code events} table
 * declares {@code PRIMARY KEY (id, created_at)} because Postgres native range
 * partitioning requires the partition key inside the PK; JPA needs a matching
 * {@code @IdClass} to mirror that.
 */
public class EventId implements Serializable {

    private Long id;
    private OffsetDateTime createdAt;

    public EventId() {}

    public EventId(Long id, OffsetDateTime createdAt) {
        this.id = id;
        this.createdAt = createdAt;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public OffsetDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(OffsetDateTime createdAt) { this.createdAt = createdAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventId other)) return false;
        return Objects.equals(id, other.id) && Objects.equals(createdAt, other.createdAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, createdAt);
    }
}
