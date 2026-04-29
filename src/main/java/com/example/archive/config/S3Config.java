package com.example.archive.config;

import com.example.archive.archive.ArchiveProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

@Configuration
@EnableConfigurationProperties({AwsProperties.class, ArchiveProperties.class})
public class S3Config {

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
}
