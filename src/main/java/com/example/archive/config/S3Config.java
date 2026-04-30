package com.example.archive.config;

import com.example.archive.archive.ArchiveProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.net.URI;

@Configuration
@EnableConfigurationProperties({AwsProperties.class, ArchiveProperties.class})
public class S3Config {

    /**
     * Sync client retained for admin/verification ops (bucket creation, object
     * GETs in integration tests). Production archiving uses {@link
     * #s3TransferManager(S3AsyncClient)} instead — this bean is intentionally
     * not on the archiving hot path.
     */
    @Bean
    public S3Client s3Client(AwsProperties aws) {
        AwsProperties.S3 s3 = aws.s3();
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(aws.region()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(aws.accessKey(), aws.secretKey())))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .forcePathStyle(s3 != null && s3.pathStyleAccess());

        String endpoint = s3 != null ? s3.endpointOverride() : null;
        if (endpoint != null && !endpoint.isBlank()) {
            builder.endpointOverride(URI.create(endpoint));
        }
        return builder.build();
    }

    /**
     * CRT-backed async client. Handles unknown-content-length streams by buffering
     * one part at a time and uploading parts in parallel. This is what removes the
     * 2 GB byte-array cap from the previous PoC implementation.
     *
     * <p>{@code destroyMethod = "close"} releases native CRT resources at JVM
     * shutdown — without it, you'd leak file descriptors / native handles on
     * context-close in tests.
     */
    @Bean(destroyMethod = "close")
    public S3AsyncClient s3AsyncClient(AwsProperties aws) {
        AwsProperties.S3 s3 = aws.s3();
        S3CrtAsyncClientBuilder builder = S3AsyncClient.crtBuilder()
                .region(Region.of(aws.region()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(aws.accessKey(), aws.secretKey())))
                .forcePathStyle(s3 != null && s3.pathStyleAccess());

        if (s3 != null && s3.partSizeMb() > 0) {
            builder.minimumPartSizeInBytes(s3.partSizeMb() * 1024L * 1024L);
        }

        String endpoint = s3 != null ? s3.endpointOverride() : null;
        if (endpoint != null && !endpoint.isBlank()) {
            builder.endpointOverride(URI.create(endpoint));
        }
        return builder.build();
    }

    @Bean(destroyMethod = "close")
    public S3TransferManager s3TransferManager(S3AsyncClient s3AsyncClient) {
        return S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
    }
}
