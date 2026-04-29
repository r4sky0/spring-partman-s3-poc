package com.example.archive.archive;

import java.time.OffsetDateTime;

public record PartitionInfo(
        String tableName,
        OffsetDateTime lowerBound,
        OffsetDateTime upperBound
) {}
