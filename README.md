# Java Coordinator

A **state-machine loop** that orchestrates the sync plane between Rust proxy servers, PostgreSQL, Redpanda (Kafka-compatible event streaming), MinIO (S3-compatible object storage), and optionally Oracle Database.

This service replaces the legacy Zig coordinator (`zig-coordinator`) with a Java 21 + Spring Boot 3 + Apache Camel implementation that brings native JDBC, Kafka protocol, and first-class observability (Micrometer, OpenTelemetry, Prometheus) to the coordinator role.

---

## What It Solves

The coordinator is the central orchestrator for SSL proxy sync events. It:

- **Ingests scan requests** from Redpanda (`sync.scan.request`) — proxy events that describe TLS/SSL scans — and records them in PostgreSQL.
- **Manages batch lifecycle** — moves events through a state machine (`pending → processing → batched → dispatched → completed/failed`) using stored procedures in the `coordinator` PostgreSQL schema.
- **Dispatches batches** to Oracle worker processes via `sync.oracle.load` and handles their results from `sync.oracle.result`.
- **Handles wireless operations** — 7 independent request/reply and fire-and-forget handlers for wireless sensor data (backlog management, MAC address lookup, authorized networks, probe flush).
- **Applies backpressure** — suspends scan consumption when the pending ledger exceeds a configurable budget (`ingest_batch_size × 4`).
- **Adaptively tunes pull windows** — shrinks Kafka `maxPollRecords` when the database falls behind.
- **Archives raw payloads** to MinIO and prunes expired data from PostgreSQL.
- **Generates shadow device alerts** at a rate-limited interval.
- **Exposes health, metrics, and tracing** via Spring Boot Actuator, Micrometer, and OpenTelemetry.

---

## Architecture

The coordinator is a **timer-driven loop** implemented as Apache Camel routes. The main loop ticks at the `coordinator.idle-sleep-ms` interval (default 250 ms) and sequences through these phases:

```
timer:coordinator-loop
  │
  ├─ direct:adaptivePull        Shrink Kafka fetch size when DB is behind
  ├─ direct:backpressureV2      Suspend scan consumer when pending count ≥ budget
  ├─ direct:processScanRequests  Lightweight pass-through (delegated to ScanRequestRoute)
  ├─ direct:processIngest        Call process_ingest_ledger() stored procedure
  ├─ direct:recoverStaleBatches  Recover dispatched-but-stale batches
  ├─ direct:dispatchBatches      Get next batch → publish to sync.oracle.load
  ├─ direct:handleResults        Lightweight pass-through (delegated to OracleResultRoute)
  ├─ direct:shadowAudit          Rate-limited shadow device alert generation
  ├─ direct:wirelessOperations   Lightweight pass-through (delegated to 7 WirelessRoutes)
  └─ direct:heartbeat            Record loop counter, route states, emit heartbeat log
```

**Independent Kafka consumer routes** (not in the main loop, but managed by Camel):

- `ScanRequestRoute` — consumes `sync.scan.request`, accumulates into batches, flushes to PostgreSQL
- `OracleResultRoute` — consumes `sync.oracle.result`, accumulates into batches, calls `process_batch_results()`
- `OracleLoadRoute` — consumes `sync.oracle.load`, performs Oracle sink operations, publishes results to `sync.oracle.result`
- `WirelessRoutes` — 7 independent consumer routes for wireless operations

**Timer-based background routes**:

- `wireless-payload-archive` — periodically archives old wireless payloads to MinIO
- `retention-prune` — periodically prunes expired events and tombstones from PostgreSQL
- `scan-request-flush-timer` — flushes partial scan record batches every 1 second
- `oracle-result-flush-timer` — flushes partial result batches every 1 second

### Route Map

| Route ID | Type | Source | Description |
|---|---|---|---|
| `coordinator-main-loop` | timer | `timer:coordinator-loop` | Drives the main state-machine loop |
| `coordinator-adaptive-pull` | direct | `direct:adaptivePull` | Shrinks Kafka fetch size when DB is behind |
| `coordinator-backpressure` | direct | `direct:backpressureV2` | Suspends/resumes scan consumer based on pending count |
| `coordinator-scan-requests` | direct | `direct:processScanRequests` | Pass-through (delegation marker) |
| `coordinator-ingest` | direct | `direct:processIngest` | Calls `coordinator.process_ingest_ledger()` |
| `coordinator-stale-recovery` | direct | `direct:recoverStaleBatches` | Calls `coordinator.recover_stale_dispatched_batches()` |
| `coordinator-dispatch` | direct | `direct:dispatchBatches` | Loops dispatching batches to `sync.oracle.load` |
| `coordinator-results` | direct | `direct:handleResults` | Pass-through (delegation marker) |
| `coordinator-shadow-audit` | direct | `direct:shadowAudit` | Rate-limited shadow alert generation |
| `coordinator-wireless` | direct | `direct:wirelessOperations` | Pass-through (delegation marker) |
| `coordinator-heartbeat` | direct | `direct:heartbeat` | Metrics + heartbeat log |
| `scan-request-consumer` | kafka | `sync.scan.request` | Consumes scan requests, records in DB |
| `scan-request-flush-timer` | timer | `timer:scan-flush` | Flushes partial scan batches every 1s |
| `oracle-load-consumer` | kafka | `sync.oracle.load` | Oracle sink worker (conditional on `oracle-sink.enabled`) |
| `oracle-result-consumer` | kafka | `sync.oracle.result` | Consumes Oracle results, calls `process_batch_results()` |
| `oracle-result-flush-timer` | timer | `timer:result-flush` | Flushes partial result batches every 1s |
| `wireless-backlog-save` | kafka | `wireless.backlog.save` | Saves wireless backlog entry |
| `wireless-backlog-list` | kafka | `wireless.backlog.list` | Lists pending backlog, publishes reply |
| `wireless-backlog-synced` | kafka | `wireless.backlog.synced` | Marks backlog entry synced |
| `wireless-backlog-prune` | kafka | `wireless.backlog.prune` | Prunes backlog, publishes reply |
| `wireless-mac-lookup` | kafka | `wireless.mac.lookup` | Looks up device by MAC, publishes reply |
| `wireless-networks-authorized` | kafka | `wireless.networks.authorized` | Lists authorized networks, publishes reply |
| `wireless-probe-flush` | kafka | `wireless.probe.flush` | Flushes probe batch |
| `wireless-payload-archive` | timer | `timer:wireless-payload-archive` | Archives old payloads to MinIO |
| `retention-prune` | timer | `timer:retention-prune` | Prunes expired events and tombstones |

---

## Redpanda Topics

The coordinator interacts with the following Redpanda topics:

### Consumed Topics

| Topic | Consumer Group | Route | Purpose |
|---|---|---|---|
| `sync.scan.request` | `zig-coordinator-scan` | `scan-request-consumer` | Ingest scan request events |
| `sync.oracle.load` | `oracle-worker-load` | `oracle-load-consumer` | Oracle sink workload |
| `sync.oracle.result` | `zig-coordinator-result` | `oracle-result-consumer` | Oracle batch results |
| `proxy.payload_audit` | `zig-coordinator-payload-audit` | (future) | Payload audit events |
| `wireless.backlog.save` | `wireless-backlog-save` | `wireless-backlog-save` | Save wireless backlog entry |
| `wireless.backlog.list` | `wireless-backlog-list` | `wireless-backlog-list` | List pending backlog |
| `wireless.backlog.synced` | `wireless-backlog-synced` | `wireless-backlog-synced` | Mark backlog synced |
| `wireless.backlog.prune` | `wireless-backlog-prune` | `wireless-backlog-prune` | Prune backlog |
| `wireless.mac.lookup` | `wireless-mac-lookup` | `wireless-mac-lookup` | MAC address lookup |
| `wireless.networks.authorized` | `wireless-networks-authorized` | `wireless-networks-authorized` | List authorized networks |
| `wireless.probe.flush` | `wireless-probe-flush` | `wireless-probe-flush` | Flush probe data |

### Produced Topics

| Topic | Producer | Purpose |
|---|---|---|
| `sync.oracle.load` | `coordinator-dispatch` route | Dispatch batches to Oracle workers |
| `sync.oracle.result` | `oracle-load-consumer` route | Oracle worker output |
| `wireless.backlog.list.reply` | `wireless-backlog-list` route | Reply to backlog list requests |
| `wireless.backlog.prune.reply` | `wireless-backlog-prune` route | Reply to backlog prune requests |
| `wireless.mac.lookup.reply` | `wireless-mac-lookup` route | Reply to MAC lookup requests |
| `wireless.networks.authorized.reply` | `wireless-networks-authorized` route | Reply to authorized networks requests |
| DLQ topics | Error handler | `{topic}.dlq` for each consumed topic |

### Stream Names

The coordinator tracks cursor positions per **stream name**. Configured streams:
- `proxy.events`
- `wireless.audit`
- `audit.wireless.bandwidth`
- `wireless.alert.rogue_ap`
- `wireless.alert.deauth_flood`
- `wireless.alert.signal_anomaly`
- `wireless.alert.pmf_attack`
- `wireless.client.inventory`
- `wireless.probe.flush`
- `proxy.payload_audit`

A subset of these (`oracle-stream-names`) are additionally dispatched to Oracle workers.

---

## Wireless Operations

Seven independent Kafka consumer routes handle wireless sensor operations. Each has its own consumer group for independent consumption and offset management.

| Handler | Pattern | DB Function |
|---|---|---|
| `backlog-save` | Fire-and-forget | `coordinator.save_backlog_entry()` |
| `backlog-list` | Request/Reply | `coordinator.list_pending_backlog()` |
| `backlog-synced` | Fire-and-forget | `coordinator.mark_backlog_synced()` |
| `backlog-prune` | Request/Reply | `coordinator.prune_backlog()` |
| `mac-lookup` | Request/Reply | `coordinator.lookup_device_by_mac()` |
| `networks-authorized` | Request/Reply | `coordinator.list_authorized_networks()` |
| `probe-flush` | Fire-and-forget | `coordinator.flush_probe_batch()` |

Request/reply handlers parse an optional `reply_topic` field from the incoming JSON payload. If provided, the reply is published to that topic; otherwise it goes to the configured default reply topic. Reply topics are validated against an allowlist.

---

## Oracle Sink

The optional Oracle sink (`oracle-sink.enabled=false` by default) provides JDBC-based Oracle Database integration:

- **Wallet-based authentication** using `TNS_ADMIN`, `ORACLE_CONN`, `ORACLE_USER`, and `ORACLE_PASS_FILE`.
- **Schema validation** with optional `warn-only` mode for graceful degradation.
- **Error classification** into permanent vs. retryable errors (`OracleErrorClass`).
- **Configurable connection pool** with HikariCP.
- **Health indicator** (`OracleHealthIndicator`) checks Oracle connectivity via the Actuator health endpoint.

When enabled, the `OracleLoadRoute` consumes from `sync.oracle.load`, performs the Oracle load operation, and publishes the result to `sync.oracle.result`.

---

## Configuration

The coordinator is configured exclusively through environment variables. All configuration is mapped to Spring Boot `@ConfigurationProperties` records:

- `CoordinatorProperties` (prefix `coordinator`) — core coordinator settings
- `OracleSinkProperties` (prefix `oracle-sink`) — Oracle JDBC settings

### Core Settings

| Variable | Default | Description |
|---|---|---|
| `SYNC_STREAM_NAME` | `proxy.events` | Primary stream name |
| `SYNC_STREAM_NAMES` | (see yaml) | Comma-separated list of all stream names |
| `SYNC_ORACLE_STREAM_NAMES` | (see yaml) | Stream names dispatched to Oracle |
| `SYNC_REDPANDA_BOOTSTRAP_SERVERS` | `localhost:9092` | Redpanda/Kafka bootstrap servers |
| `DATABASE_URL` | `jdbc:postgresql://localhost:5432/sync` | PostgreSQL JDBC URL |
| `POSTGRES_USER` | `sync` | PostgreSQL username |
| `POSTGRES_PASSWORD` | *(required)* | PostgreSQL password |
| `COORDINATOR_DB_POOL` | `35` | HikariCP maximum pool size |
| `COORDINATOR_DB_POOL_MIN_IDLE` | `10` | HikariCP minimum idle connections |

### Redpanda Topics

| Variable | Default | Description |
|---|---|---|
| `SYNC_SCAN_TOPIC` | `sync.scan.request` | Scan request topic |
| `SYNC_LOAD_TOPIC` | `sync.oracle.load` | Oracle load topic |
| `SYNC_RESULT_TOPIC` | `sync.oracle.result` | Oracle result topic |
| `SYNC_PAYLOAD_AUDIT_TOPIC` | `proxy.payload_audit` | Payload audit topic |
| `WIRELESS_BACKLOG_SAVE_TOPIC` | `wireless.backlog.save` | Backlog save topic |
| `WIRELESS_BACKLOG_LIST_TOPIC` | `wireless.backlog.list` | Backlog list topic |
| `WIRELESS_BACKLOG_SYNCED_TOPIC` | `wireless.backlog.synced` | Backlog synced topic |
| `WIRELESS_BACKLOG_PRUNE_TOPIC` | `wireless.backlog.prune` | Backlog prune topic |
| `WIRELESS_MAC_LOOKUP_TOPIC` | `wireless.mac.lookup` | MAC lookup topic |
| `WIRELESS_NETWORKS_AUTHORIZED_TOPIC` | `wireless.networks.authorized` | Networks authorized topic |
| `WIRELESS_PROBE_FLUSH_TOPIC` | `wireless.probe.flush` | Probe flush topic |

### Consumer Groups

| Variable | Default | Description |
|---|---|---|
| `SYNC_SCAN_CONSUMER` | `zig-coordinator-scan` | Scan request consumer group |
| `SYNC_LOAD_CONSUMER` | `oracle-worker-load` | Oracle load consumer group |
| `SYNC_RESULT_CONSUMER` | `zig-coordinator-result` | Oracle result consumer group |
| `SYNC_PAYLOAD_AUDIT_CONSUMER` | `zig-coordinator-payload-audit` | Payload audit consumer group |
| `WIRELESS_BACKLOG_SAVE_CONSUMER` | `wireless-backlog-save` | Backlog save consumer group |
| `WIRELESS_BACKLOG_LIST_CONSUMER` | `wireless-backlog-list` | Backlog list consumer group |
| `WIRELESS_BACKLOG_SYNCED_CONSUMER` | `wireless-backlog-synced` | Backlog synced consumer group |
| `WIRELESS_BACKLOG_PRUNE_CONSUMER` | `wireless-backlog-prune` | Backlog prune consumer group |
| `WIRELESS_MAC_LOOKUP_CONSUMER` | `wireless-mac-lookup` | MAC lookup consumer group |
| `WIRELESS_NETWORKS_AUTHORIZED_CONSUMER` | `wireless-networks-authorized` | Networks authorized consumer group |
| `WIRELESS_PROBE_FLUSH_CONSUMER` | `wireless-probe-flush` | Probe flush consumer group |

### Backpressure & Tuning

| Variable | Default | Description |
|---|---|---|
| `SYNC_INGEST_BATCH_SIZE` | `1000` | Ingest batch size |
| `SYNC_DISPATCH_BATCH_SIZE` | `50` | Dispatch batch size |
| `SYNC_BACKPRESSURE_BUDGET_MULTIPLIER` | `4` | Budget = `ingest_batch_size × multiplier` |
| `SYNC_IDLE_SLEEP_MS` | `250` | Main loop interval |
| `SYNC_IDLE_SLEEP_BACKOFF_MS` | `1000` | Backoff when idle |
| `SYNC_SCAN_FETCH_COUNT` | `500` | Kafka `maxPollRecords` for scan consumer |
| `SYNC_RESULT_FETCH_COUNT` | `200` | Kafka `maxPollRecords` for result consumer |
| `SYNC_SCAN_CONSUMERS_COUNT` | `1` | Concurrent consumers for scan topic |
| `SYNC_RESULT_CONSUMERS_COUNT` | `1` | Concurrent consumers for result topic |
| `WIRELESS_CONSUMERS_COUNT` | `1` | Concurrent consumers per wireless handler |
| `WIRELESS_MAX_POLL_RECORDS` | `1` | Max poll records for wireless handlers |
| `SYNC_ADAPTIVE_PULL_CHANGE_THRESHOLD` | `50` | Threshold for adaptive pull adjustments |

### Retention & Archiving

| Variable | Default | Description |
|---|---|---|
| `WIRELESS_RAW_ARCHIVE_ENABLED` | `true` | Enable archiving old payloads to MinIO |
| `WIRELESS_RAW_PAYLOAD_HOT_DAYS` | `7` | Days before payloads are eligible for archiving |
| `SYNC_EVENT_ROW_RETENTION_DAYS` | `30` | Event row retention |
| `SYNC_EVENT_TOMBSTONE_RETENTION_DAYS` | `45` | Tombstone retention |
| `WIRELESS_RAW_ARCHIVE_BATCH_SIZE` | `100` | Batch size for archive operations |
| `RETENTION_PRUNE_BATCH_SIZE` | `5000` | Batch size for retention pruning |
| `WIRELESS_RAW_ARCHIVE_INTERVAL_MS` | `300000` (5 min) | Archive cycle interval |
| `RETENTION_MAINTENANCE_INTERVAL_MS` | `3600000` (1 hr) | Retention prune interval |
| `WIRELESS_RAW_ARCHIVE_BUCKET` | `ssl-proxy-wireless-raw-archive` | MinIO bucket name |
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO server endpoint |
| `MINIO_ACCESS_KEY_ID` | *(required when archive enabled)* | MinIO access key |
| `MINIO_SECRET_ACCESS_KEY` | *(required when archive enabled)* | MinIO secret key |

### Oracle Sink

| Variable | Default | Description |
|---|---|---|
| `ORACLE_SINK_ENABLED` | `false` | Enable Oracle sink |
| `ORACLE_SINK_WIRELESS_ENABLED` | `true` | Enable wireless Oracle objects |
| `ORACLE_SINK_SCHEMA_VALIDATION_WARN_ONLY` | `false` | Schema validation mode |
| `ORACLE_CONN` | *(required when enabled)* | Oracle TNS alias |
| `ORACLE_JDBC_URL` | *(alternative)* | Direct JDBC URL |
| `ORACLE_USER` | *(required when enabled)* | Oracle username |
| `ORACLE_PASS_FILE` | *(required when enabled)* | Path to Oracle password file |
| `TNS_ADMIN` | *(required when enabled)* | Oracle wallet directory |
| `ORACLE_COORDINATOR_POOL` | `8` | HikariCP pool size |
| `ORACLE_LOAD_MAX_RETRIES` | `3` | Max retries for Oracle load |

### Observability

| Variable | Default | Description |
|---|---|---|
| `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` | `health,info,metrics,hikaricp,prometheus` | Actuator endpoints |
| `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` | `0.05` | OpenTelemetry sampling rate |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP trace endpoint |

---

## Building

### Prerequisites

- JDK 21 (Azul Zulu or compatible)
- Gradle (optional; wrapper included)

### Build the JAR

```sh
./gradlew build
```

This compiles, runs tests, and produces a fat JAR at `build/libs/java-coordinator-0.1.0.jar`.

### Run Tests

```sh
./gradlew test
```

Tests include:
- Unit tests for processors, services, and utility classes
- SQL contract tests that validate stored procedure calls
- Testcontainers-based integration tests (PostgreSQL, Kafka)

### Build Docker Image

```sh
./gradlew bootBuildImage
```

Uses Paketo Buildpacks to produce `ssl-proxy/java-coordinator:0.1.0`.

Or use the Dockerfile directly:

```sh
docker build -t ssl-proxy/java-coordinator:0.1.0 .
```

---

## Running

### Local Development

```sh
# Start dependencies (PostgreSQL, Redpanda, MinIO)
docker compose up -d postgres redpanda minio

# Set required env vars and run
export DATABASE_URL=jdbc:postgresql://localhost:5432/sync
export POSTGRES_USER=sync
export POSTGRES_PASSWORD=...
export SYNC_REDPANDA_BOOTSTRAP_SERVERS=localhost:9092
export MINIO_ACCESS_KEY_ID=...
export MINIO_SECRET_ACCESS_KEY=...

./gradlew bootRun
```

### Docker

```sh
docker run -d \
  -p 8081:8081 \
  -e DATABASE_URL=jdbc:postgresql://host.docker.internal:5432/sync \
  -e POSTGRES_USER=sync \
  -e POSTGRES_PASSWORD=... \
  -e SYNC_REDPANDA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  ssl-proxy/java-coordinator:0.1.0
```

### Kubernetes / Helm

The coordinator is deployed as part of the `ssl-proxy` Helm chart (located in the parent repository at `helm/ssl-proxy/`). A typical values override:

```yaml
java-coordinator:
  enabled: true
  image:
    repository: ssl-proxy/java-coordinator
    tag: 0.1.0
  env:
    DATABASE_URL: jdbc:postgresql://postgres:5432/sync
    POSTGRES_USER: sync
    POSTGRES_PASSWORD:
      valueFrom:
        secretKeyRef:
          name: postgres-credentials
          key: password
    SYNC_REDPANDA_BOOTSTRAP_SERVERS: redpanda:9092
    MINIO_ENDPOINT: http://minio:9000
    MINIO_ACCESS_KEY_ID:
      valueFrom:
        secretKeyRef:
          name: minio-credentials
          key: access-key
    MINIO_SECRET_ACCESS_KEY:
      valueFrom:
        secretKeyRef:
          name: minio-credentials
          key: secret-key
```

---

## Adding a New Redpanda Topic

Here is the complete workflow for wiring a new Redpanda topic into the coordinator.

### 1. Bootstrap the Topic

Add the topic to the `topics.manifest` file in the parent repo (`docker/redpanda/topics.manifest`), then run the bootstrap script:

```sh
export SYNC_REDPANDA_BOOTSTRAP_SERVERS=redpanda:9092
./setup-wireless-redpanda.sh
```

The topic will be created with the appropriate partition count and replication factor as defined in the manifest.

### 2. Add Configuration Properties

In `CoordinatorProperties.java`, add topic and consumer group fields:

```java
public record CoordinatorProperties(
    // ... existing fields ...

    // === New topic ===
    String newTopicName,          // topic name
    String newTopicConsumer,      // consumer group

    // === Optional reply topic ===
    String newTopicReplyTopic     // if using request/reply pattern
) {
```

Add defaults to the `DEFAULTS` instance and update the constructor call.

### 3. Add Environment Variable Defaults

In `src/main/resources/application.yaml`:

```yaml
coordinator:
  # ... existing ...
  new-topic-name: ${NEW_TOPIC_NAME:your.new.topic}
  new-topic-consumer: ${NEW_TOPIC_CONSUMER:your-consumer-group}
  new-topic-reply-topic: ${NEW_TOPIC_REPLY_TOPIC:your.new.topic.reply}
```

### 4. Create the Camel Route

Create a new route class (or add to `WirelessRoutes` if it's a wireless operation):

```java
@Component
public class NewTopicRoute extends RouteBuilder {

    private final CoordinatorProperties props;
    private final DatabaseService db;

    public NewTopicRoute(CoordinatorProperties props, DatabaseService db) {
        this.props = props;
        this.db = db;
    }

    @Override
    public void configure() {
        // Error handling with DLQ
        onException(Exception.class)
                .maximumRedeliveries(3)
                .redeliveryDelay(500)
                .useOriginalMessage()
                .handled(true)
                .to("kafka:" + props.newTopicName() + ".dlq");

        // Consumer route
        from("kafka:" + props.newTopicName()
                + "?groupId=" + props.newTopicConsumer()
                + "&autoOffsetReset=earliest"
                + "&maxPollRecords=500"
                + "&breakOnFirstError=true")
        .routeId("new-topic-consumer")
        .process(exchange -> {
            // Your processing logic
            String body = exchange.getIn().getBody(String.class);
            // ... call db methods ...
        });
    }
}
```

### 5. For Request/Reply Patterns

If the new topic follows a request/reply pattern (like the wireless MAC lookup), add a reply topic and follow the pattern in `WirelessRoutes`:

```java
from(wirelessConsumerUri(props.newTopicName(), props.newTopicConsumer()))
.routeId("new-topic-consumer")
.setProperty("dlqTopic", constant(props.newTopicName() + ".dlq"))
.process(replyingHandler(
        payload -> {
            // ... process request, return reply JSON ...
            return result;
        },
        props.newTopicReplyTopic(),
        "new_topic_event"
));
```

### 6. Register in the Main Loop (if needed)

If the new topic needs timing parity with the main loop (e.g., it should be polled each iteration), add a pass-through endpoint in `CoordinatorRoute.configure()`:

```java
from("direct:newTopicOperation")
    .routeId("coordinator-new-topic")
    .log(LoggingLevel.TRACE, "event=new_topic status=delegated");
```

And add `.to("direct:newTopicOperation")` to the main loop chain.

### 7. Add to Stream Names (if applicable)

If the new topic represents a new event stream that needs cursor tracking, add it to `SYNC_STREAM_NAMES` and/or `SYNC_ORACLE_STREAM_NAMES`.

### 8. Update Backpressure (if applicable)

If the new topic contributes to backpressure (derived from the `streamNames` list), it is automatically accounted for in the `pendingLedgerCount()` DB query.

---

## Helm Deployment

The coordinator is deployed via the parent repo's `helm/ssl-proxy/` chart. Key configuration points:

- **ConfigMap**: Environment variables are typically set via a ConfigMap or `values.yaml` env overrides.
- **Secrets**: Database passwords, MinIO credentials, and Oracle passwords should be mounted from Kubernetes Secrets.
- **Oracle wallet**: The `TNS_ADMIN` directory (`/app/wallet`) can be mounted from a Secret or PVC.
- **Readiness/Health**: Spring Boot Actuator health endpoint is checked via the Dockerfile `HEALTHCHECK` and Kubernetes liveness/readiness probes.
- **Graceful shutdown**: The coordinator handles `SIGTERM` by suspending all routes and draining in-flight exchanges with a 30-second timeout.

---

## Metrics & Observability

| Endpoint | Description |
|---|---|
| `/actuator/health` | Health check (DB, Oracle, Redpanda via Kafka consumer status) |
| `/actuator/metrics` | Prometheus metrics (Micrometer) |
| `/actuator/prometheus` | Prometheus scrape endpoint |
| `/actuator/hikaricp` | Connection pool metrics |
| `/actuator/info` | Application info |

### Key Metrics (Micrometer)

| Metric | Type | Description |
|---|---|---|
| `db.client.operation` | Timer | Database stored procedure call duration with `db.operation` tag |
| `coordinator.loop.counter` | Counter | Main loop iterations |
| `coordinator.batch.dispatched` | Counter | Successfully dispatched batches |
| `coordinator.route.state` | Gauge | Route started/suspended state |
| `coordinator.backpressure.active` | Gauge | Whether scan consumer is suspended |
| `coordinator.backpressure.pending_count` | Gauge | Current pending ledger count |

### Tracing (OpenTelemetry)

OTLP traces are exported to the endpoint configured by `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4317` gRPC). Sampling rate is 5% by default.

### Health Checks

- `DatabaseService.checkConnectivity()` — PostgreSQL `SELECT 1`
- `OracleHealthIndicator` — Oracle connection validation (when enabled)
- Route state monitoring — scan and result consumer routes tracked in heartbeat metrics

---

## Testing

```sh
# Run all tests
./gradlew test

# Run a specific test class
./gradlew test --tests "com.sslproxy.coordinator.route.OracleLoadRouteTest"
```

The test suite uses:
- **JUnit 5** — test framework
- **Spring Boot test** — application context loading
- **Camel test support** — route testing with `camel-test-spring-junit5`
- **Testcontainers** — PostgreSQL and Kafka containers for integration tests
- **SQL contract tests** — validate stored procedure call signatures against the root `sql/` files

---

## Project Structure

```
services/zig-coordinator/
├── build.gradle.kts                   # Gradle build (Java 21, Spring Boot 3, Camel 4)
├── settings.gradle.kts                # Root project name: java-coordinator
├── Dockerfile                         # Multi-stage build, slim JRE runtime
├── setup-wireless-redpanda.sh         # Redpanda topic bootstrap wrapper
├── gradlew / gradlew.bat              # Gradle wrapper
├── src/
│   ├── main/
│   │   ├── java/com/sslproxy/coordinator/
│   │   │   ├── CoordinatorApplication.java        # @SpringBootApplication entry point
│   │   │   ├── config/
│   │   │   │   ├── CoordinatorProperties.java     # ~50 env vars as @ConfigurationProperties
│   │   │   │   ├── OracleSinkProperties.java      # Oracle JDBC configuration
│   │   │   │   ├── DataSourceConfig.java          # PostgreSQL HikariCP datasource
│   │   │   │   ├── DataSourceDiagnostics.java     # Connection pool diagnostics
│   │   │   │   ├── JacksonConfig.java             # ObjectMapper configuration
│   │   │   │   └── OtlpTracingEndpointEnvironmentPostProcessor.java
│   │   │   ├── fp/                                # Functional programming utilities
│   │   │   │   ├── BoundedAccumulator.java         # Batch accumulator (ingest, results)
│   │   │   │   ├── DbResult.java                   # Result monad for DB operations
│   │   │   │   ├── FpUtils.java                    # Partition, chunk helpers
│   │   │   │   ├── RouteAdjustment.java            # Consumer property adjustment
│   │   │   │   ├── SqlArrays.java                  # JDBC array helpers
│   │   │   │   ├── WirelessHandler.java            # Wireless operation handler interface
│   │   │   │   └── CheckedConsumer.java
│   │   │   ├── model/                             # DTOs
│   │   │   │   ├── ScanRequest.java
│   │   │   │   ├── DispatchPayload.java
│   │   │   │   └── WirelessRequest.java
│   │   │   ├── oracle/                            # Oracle sink
│   │   │   │   ├── OracleSink.java / JdbcOracleSink.java
│   │   │   │   ├── OracleLoad.java / OracleLoadHandler.java
│   │   │   │   ├── OracleResult.java
│   │   │   │   ├── OracleConnectionFactory.java
│   │   │   │   ├── OracleErrorClass.java
│   │   │   │   ├── OracleChecksum.java
│   │   │   │   ├── OracleClock.java
│   │   │   │   ├── OracleHealthIndicator.java
│   │   │   │   ├── OraclePayloadResolver.java
│   │   │   │   ├── OracleTransformService.java
│   │   │   │   ├── OracleRows.java / OracleSinkTarget.java
│   │   │   │   └── JsonFields.java
│   │   │   ├── processor/                         # Processors (Camel exchange processing)
│   │   │   │   ├── BatchDispatchProcessor.java
│   │   │   │   ├── CoordinatorProcessors.java
│   │   │   │   ├── PayloadAuditRecordProcessor.java
│   │   │   │   ├── PayloadResolver.java
│   │   │   │   ├── ResultProcessor.java
│   │   │   │   └── ScanRecordProcessor.java
│   │   │   ├── route/                             # Camel routes
│   │   │   │   ├── CoordinatorRoute.java          # Main state-machine loop
│   │   │   │   ├── ScanRequestRoute.java          # sync.scan.request consumer
│   │   │   │   ├── OracleLoadRoute.java           # sync.oracle.load consumer (Oracle sink)
│   │   │   │   ├── OracleResultRoute.java         # sync.oracle.result consumer
│   │   │   │   ├── PayloadAuditRoute.java         # proxy.payload_audit consumer
│   │   │   │   ├── RetentionMaintenanceRoute.java # Archive + prune timers
│   │   │   │   └── WirelessRoutes.java            # 7 wireless handlers
│   │   │   └── service/                           # Business logic services
│   │   │       ├── DatabaseService.java           # All stored procedure calls
│   │   │       ├── CursorService.java             # Cursor management
│   │   │       ├── BackpressureService.java       # Backpressure + consumer suspend/resume
│   │   │       ├── AdaptivePullController.java    # Dynamic maxPollRecords adjustment
│   │   │       ├── CoordinatorMetricsService.java # Micrometer metrics
│   │   │       ├── RedpandaLagMetricsService.java # Redpanda consumer lag
│   │   │       ├── PayloadArchiveService.java     # MinIO archiving + retention
│   │   │       └── HealthCheckService.java        # Startup + readiness checks
│   │   └── resources/
│   │       ├── application.yaml                   # Main configuration
│   │       ├── logback.xml                        # Logging configuration
│   │       └── MIGRATION.md                       # Zig→Java migration notes
│   └── test/java/com/sslproxy/coordinator/
│       ├── config/
│       ├── fp/
│       ├── oracle/
│       ├── processor/
│       ├── route/
│       ├── service/
│       └── testsupport/
```

---

## License

Proprietary — SSL Proxy internal service.