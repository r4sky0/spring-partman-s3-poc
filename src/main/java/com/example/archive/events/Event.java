package com.example.archive.events;

import java.time.OffsetDateTime;
import java.util.UUID;

public record Event(
        UUID tenantId,
        String eventType,
        String payloadJson,
        OffsetDateTime createdAt
) {}
