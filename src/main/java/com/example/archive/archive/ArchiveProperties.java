package com.example.archive.archive;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "archive")
public record ArchiveProperties(
        Duration retention,
        String bucket,
        Duration interval,
        Format format
) {
    /** Output format for archived partitions. */
    public enum Format {
        /** Gzipped CSV — universal, simple, but not directly queryable from analytics tools. */
        CSV,
        /** Apache Parquet — columnar, queryable from Athena/DuckDB/Spark via S3 SELECT. */
        PARQUET
    }

    public ArchiveProperties {
        if (format == null) format = Format.CSV;
    }
}
