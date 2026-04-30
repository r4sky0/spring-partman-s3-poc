package com.example.archive.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "aws")
public record AwsProperties(
        String region,
        String accessKey,
        String secretKey,
        S3 s3
) {
    /**
     * @param partSizeMb minimum CRT multipart part size, in MiB. {@code 0} (the
     *                   binding default for an unset property) leaves the CRT
     *                   default in place (8 MiB). Increase for high-throughput
     *                   links; decrease for memory-constrained environments
     *                   (each in-flight part is buffered before it's flushed).
     */
    public record S3(String endpointOverride, boolean pathStyleAccess, int partSizeMb) {}
}
