package com.example.archive;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;

/**
 * Wires the two PoC containers into the Spring context.
 * <p>
 * Postgres uses a custom image (built on the fly via ImageFromDockerfile from
 * docker/postgres/Dockerfile) so pg_partman is present. The image is tagged
 * compatible with "postgres" so Testcontainers' PostgreSQLContainer recognises it
 * and Spring Boot's @ServiceConnection auto-wires the JDBC DataSource.
 * <p>
 * LocalStack S3 isn't covered by Spring Boot's awssdk v2 connection detail
 * autoconfig, so we bridge its endpoint into our own {@code aws.*} properties
 * via DynamicPropertyRegistrar (Spring Framework 6.2+).
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgresContainer() {
        DockerImageName image = DockerImageName.parse(
                new ImageFromDockerfile("archive-poc/postgres-partman:test", false)
                        .withDockerfile(Path.of("docker/postgres/Dockerfile"))
                        .get()
        ).asCompatibleSubstituteFor("postgres");

        return new PostgreSQLContainer<>(image)
                .withDatabaseName("archive")
                .withUsername("archive")
                .withPassword("archive");
    }

    @Bean
    LocalStackContainer localStackContainer() {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                .withServices(Service.S3);
    }

    @Bean
    DynamicPropertyRegistrar localStackProperties(LocalStackContainer ls) {
        return registry -> {
            registry.add("aws.s3.endpoint-override", () -> ls.getEndpointOverride(Service.S3).toString());
            registry.add("aws.region", ls::getRegion);
            registry.add("aws.access-key", ls::getAccessKey);
            registry.add("aws.secret-key", ls::getSecretKey);
            registry.add("aws.s3.path-style-access", () -> "true");
            // Make scheduled archiving effectively no-op during tests; we drive runOnce() explicitly.
            registry.add("archive.interval", () -> "1h");
        };
    }
}
