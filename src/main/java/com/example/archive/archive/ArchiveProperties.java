package com.example.archive.archive;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "archive")
public record ArchiveProperties(
        Duration retention,
        String bucket,
        Duration interval
) {}
