package com.example.archive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.TimeZone;

@SpringBootApplication
@EnableScheduling
public class ArchivePocApplication {

    public static void main(String[] args) {
        // Pin the JVM (and therefore the JDBC driver's default session timezone)
        // to UTC. Partition bounds are computed by pg_partman as
        // date_trunc('day', now()), which depends on session timezone — running
        // in CEST would yield bounds at +02 boundaries while UTC replicas would
        // expect UTC boundaries, leading to subtle overlap and routing errors.
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SpringApplication.run(ArchivePocApplication.class, args);
    }
}
