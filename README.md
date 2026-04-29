# Spring + pg_partman + LocalStack S3 Archive PoC

Demonstrates the **tiered-storage / cold-archive** pattern for a high-volume
time-series table:

- An `events` table is **range-partitioned by day** and managed by **`pg_partman`**.
- After a configurable retention window (**7 days** by default), each old daily
  partition is **exported to S3 as gzipped CSV** and then **detached + dropped**.
- The full lifecycle (ingest → partition → archive → drop) is exercised
  end-to-end against **LocalStack** by an integration test using
  **Testcontainers**.

The production `aws_s3` PostgreSQL extension is intentionally **not** used.
The export is performed by Spring via AWS SDK v2. The `PartitionArchiver`
interface is the seam: a future `Aws3ExtensionArchiver` (Aurora-only) would
slot in without touching the orchestration.

## Prerequisites

- **Java 25** (e.g. via SDKMAN: `sdk install java 25.0.3-tem`)
- **Docker Desktop** running

If you only want to verify the **build** (compile + unit tests) without
installing a JDK, you can use the Maven Docker image — see [Building without a
local JDK](#building-without-a-local-jdk).

## Run it

```bash
# Boot Postgres-with-pg_partman + LocalStack, run the app
./mvnw spring-boot:run
```

Spring Boot's `spring-boot-docker-compose` integration auto-starts the two
services from `compose.yaml` on launch and shuts them down with the app.

In another terminal, drive the demo:

```bash
# 1. Seed 14 backdated days of events (50 rows per day is plenty for a demo)
curl -X POST 'http://localhost:8080/events/seed?days=14&perDay=50'

# 2. Confirm the daily partitions exist
docker exec -it $(docker ps -qf name=postgres) \
  psql -U archive archive -c "\dt events_p*"

# 3. Trigger an archive cycle
curl -X POST http://localhost:8080/archive/run
# → {"partitionsArchived":7,"rowsArchived":350,"bytesUploaded":...}

# 4. List the gzipped CSV objects in S3
docker run --rm --network host amazon/aws-cli \
  --endpoint-url http://localhost:4566 \
  s3 ls s3://archive-bucket/events/ --recursive

# 5. Confirm the 7 archived child tables are gone
docker exec -it $(docker ps -qf name=postgres) \
  psql -U archive archive -c "\dt events_p*"

docker exec -it $(docker ps -qf name=postgres) \
  psql -U archive archive -c "SELECT * FROM archive_log;"

# 6. Inspect metrics
curl http://localhost:8080/actuator/metrics/archive.partitions.archived
```

## Run the tests

```bash
./mvnw test
```

This runs both:

- `SpringJdbcS3ArchiverUnitTest` — pure-Java, no containers (S3 key derivation, etc.)
- `ArchiveLifecycleIntegrationTest` — `@SpringBootTest` that spins up the
  Postgres-with-partman image (built on the fly from `docker/postgres/Dockerfile`)
  and a LocalStack S3 container via Testcontainers, seeds 14 daily partitions,
  runs `archiveJob.runOnce()` twice, and asserts:
  - exactly 7 partitions archived to S3 (Hive-style keys)
  - each gunzipped CSV has the expected row count
  - `archive_log` records 7 archived partitions
  - the 7 archived child tables are dropped
  - a second run is a no-op (idempotency)

## Architecture at a glance

```
┌────────────────────┐    JDBC    ┌──────────────────────────┐
│ Spring Boot 3.5.x  │ ─────────▶ │ Postgres 17 + pg_partman │
│ Java 25, virtual   │            │   events (parent)        │
│ threads, JdbcClient│            │   ├─ events_pYYYY_MM_DD  │
│ AWS SDK v2 S3      │            │   └─ ...                 │
│                    │            └──────────────────────────┘
│ POST /events       │
│ POST /events/seed  │
│ POST /archive/run  │  S3 PUT  ┌──────────────────┐
│ @Scheduled job     │ ──────▶  │ LocalStack S3    │
│                    │          │ archive-bucket/  │
│                    │          │   events/year=…  │
└────────────────────┘          └──────────────────┘
```

## Project layout

| Path | Purpose |
|------|---------|
| `src/main/resources/db/migration/` | Flyway migrations (events table, partman config, archive_log) |
| `src/main/java/.../events/` | `Event` record, `EventService`, `EventController` |
| `src/main/java/.../archive/` | `PartitionArchiver` interface, `SpringJdbcS3Archiver`, `PartitionDiscoveryService`, `PartitionArchiveJob`, `ArchiveController` |
| `src/main/java/.../config/` | `S3Config`, `AwsProperties`, `AppConfig` |
| `docker/postgres/Dockerfile` | `FROM postgres:17` + `postgresql-17-partman` |
| `docker/postgres/init/` | `CREATE EXTENSION pg_partman` (runs on first volume init) |
| `docker/localstack/init-bucket.sh` | Creates `archive-bucket` once LocalStack is ready |
| `compose.yaml` | postgres + localstack services |
| `src/test/java/.../ArchiveLifecycleIntegrationTest.java` | The headline E2E test |
| `src/test/java/.../TestcontainersConfiguration.java` | Wires the two containers as Spring beans |

## Building without a local JDK

The build can be verified entirely inside the Maven Docker image:

```bash
# Compile + unit tests (no Java needed on host)
docker run --rm \
  -v "$PWD":/work -w /work \
  -v "$HOME/.m2":/root/.m2 \
  maven:3.9-eclipse-temurin-21 \
  mvn -B test -Dtest=SpringJdbcS3ArchiverUnitTest
```

The full integration test (`ArchiveLifecycleIntegrationTest`) needs
Testcontainers talking to Docker Desktop's actual engine socket, which means
running it on the host rather than nested inside another container:

```bash
sdk install java 25.0.3-tem
./mvnw test
```

## Configuration

`application.yml` knobs:

| Property | Default | Meaning |
|---|---|---|
| `archive.retention` | `P7D` | Partitions older than this get archived |
| `archive.bucket` | `archive-bucket` | S3 bucket for archived CSV.gz |
| `archive.interval` | `60s` | `@Scheduled` cadence for the archive job |
| `aws.s3.endpoint-override` | `http://localhost:4566` | LocalStack; unset for real AWS |
| `aws.s3.path-style-access` | `true` | Required by LocalStack |

## Out of scope

- Aurora `aws_s3.query_export_to_s3`-based archiver — the `PartitionArchiver`
  interface is the extension point.
- Restore-from-S3 path.
- Parquet output (CSV.gz only).
- Auth, multi-tenant isolation, rate limiting.
