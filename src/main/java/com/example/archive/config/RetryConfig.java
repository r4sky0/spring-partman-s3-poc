package com.example.archive.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.support.RetryTemplate;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;

@Configuration
public class RetryConfig {

    /**
     * Retries transient S3 / network failures inside a single archive cycle so a
     * blip doesn't push the partition's recovery out by a full scheduled interval
     * (≥ 60 s, often longer). Three attempts with exponential backoff (500 ms →
     * 1 s → 2 s, capped at 5 s) covers the typical CRT-internal-retry-then-fail
     * timing window without holding the partition's connection for too long.
     *
     * <p>Scope: the {@code archiver.archive(p)} call only. The DDL block (audit-log
     * insert + DETACH + DROP) intentionally does NOT retry — DETACH is not idempotent
     * and the existing per-partition firebreak already handles re-pickup on the next
     * cycle (the partition stays a candidate until {@code archive_log} records it).
     */
    @Bean
    public RetryTemplate archiveRetryTemplate() {
        return RetryTemplate.builder()
                .maxAttempts(3)
                .exponentialBackoff(500, 2.0, 5000)
                .retryOn(S3Exception.class)
                .retryOn(IOException.class)
                .retryOn(TransientDataAccessException.class)
                .build();
    }
}
