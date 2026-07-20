# TiDB Local Development Environment

Standalone TiDB instance for migrating the coordinator's Oracle sink to TiDB.

## Quick Start

```bash
# Start TiDB
docker compose up -d

# Wait for health check to pass
docker compose ps

# Apply the schema
mysql -h 127.0.0.1 -P 4000 -u root < init/001_schema.sql

# Verify
mysql -h 127.0.0.1 -P 4000 -u root -e "USE coordinator; SHOW TABLES;"
```

## Connection Details

| Property | Value |
|---|---|
| Host | `127.0.0.1` |
| Port | `4000` |
| User | `root` |
| Password | (none) |
| Database | `coordinator` |
| Protocol | MySQL (TiDB wire-compatible) |

### Coordinator JDBC URL

```
jdbc:mysql://127.0.0.1:4000/coordinator?rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true
```

## Schema

Ten tables translated from Oracle to TiDB-compatible MySQL DDL:

| # | Table | Source Oracle DDL |
|---|---|---|
| 1 | `PROXY_EVENTS` | `002_proxy_events.sql` |
| 2 | `PROXY_BLOCKED_HOST_ROLLUPS` | `003_proxy_blocked_host_rollups.sql` |
| 3 | `PROXY_PAYLOAD_AUDIT` | `004_proxy_payload_audit.sql` |
| 4 | `WIRELESS_SENSORS` | `016_wireless_sensors.sql` |
| 5 | `WIRELESS_AUDIT_FRAMES` | `019_wireless_audit_frames.sql` |
| 6 | `WIRELESS_BANDWIDTH_WINDOWS` | `020_wireless_bandwidth_windows.sql` |
| 7 | `WIRELESS_ALERTS` | `021_wireless_alerts.sql` |
| 8 | `WIRELESS_ALERTS_LEDGER` | `022_wireless_alerts_ledger.sql` |
| 9 | `WIRELESS_CLIENT_INVENTORY` | `023_wireless_client_inventory.sql` |
| 10 | `WIRELESS_PROBE_REQUESTS` | `024_wireless_probe_requests.sql` |

## Oracle → TiDB Translation Notes

### Types

| Oracle | TiDB |
|---|---|
| `VARCHAR2(n)` | `VARCHAR(n)` |
| `NUMBER(1)` / `NUMBER(1,0)` | `TINYINT(1)` |
| `NUMBER(3,0)` / `NUMBER(4,0)` | `SMALLINT` |
| `NUMBER(5,0)` / `NUMBER(6,0)` / `NUMBER(10,0)` | `INT` |
| `NUMBER(12,0)` / `NUMBER(20)` / `NUMBER(20,0)` | `BIGINT` |
| `NUMBER(10,4)` | `DECIMAL(10,4)` |
| `RAW(16)` | `BINARY(16)` |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP(6)` |
| `TIMESTAMP(3)` | `TIMESTAMP(3)` |
| `DATE` | `DATE` |
| `JSON` | `JSON` |
| `CLOB`-equivalent (VARCHAR2(2000)) | `VARCHAR(2000)` |

### Oracle Features Removed

- **`BLOCKCHAIN TABLE`** — TiDB has no blockchain/immutable table feature. `PROXY_PAYLOAD_AUDIT` and `WIRELESS_ALERTS_LEDGER` are regular tables.
- **`NO DROP UNTIL 16 DAYS IDLE` / `NO DELETE LOCKED` / `HASHING USING SHA2_512 VERSION V1`** — Oracle-specific table properties.
- **`GENERATED ALWAYS AS (...) VIRTUAL`** — Virtual columns used for UTC date extraction in partitioning. Removed since TiDB doesn't support them in the same way.
- **`PARTITION BY RANGE ... INTERVAL`** — Oracle interval partitioning. Removed for local dev simplicity.
- **`RELY DISABLE NOVALIDATE`** — Oracle-specific FK constraint hint.
- **`FOREIGN KEY ... REFERENCES WIRELESS_CHANNELS`** — The `WIRELESS_CHANNELS` reference table was not migrated (it's a lookup table not used by the coordinator's write path).

### Oracle Procedures (Not Migrated)

TiDB does not support stored procedures. The two Oracle procedures must be inlined into `JdbcOracleSink`:

1. **`WIRELESS_UPSERT_SENSOR`** → `INSERT ... ON DUPLICATE KEY UPDATE` in `insertWirelessAuditFrames()`
2. **`WIRELESS_MERGE_BANDWIDTH_ALERTS`** → Java-side aggregation + `INSERT ... ON DUPLICATE KEY UPDATE` in `insertWirelessBandwidth()`

### TiDB-Specific Considerations for the Java Sink

1. **No stored procedures** — All logic must be in Java.
2. **Optimistic transactions** — TiDB uses Percolator model. Write conflicts surface as errors on `commit()`, not blocking waits. Add retry-on-conflict logic with exponential backoff.
3. **Batch inserts** — Use `rewriteBatchedStatements=true` in the JDBC URL and `PreparedStatement.addBatch()` / `executeBatch()` for throughput.
4. **`ON DUPLICATE KEY UPDATE`** — Replaces Oracle `MERGE`. Requires unique indexes on the merge key columns (already defined in the schema).
5. **`TIMESTAMP` behavior** — TiDB `TIMESTAMP` type has range `1970-01-01` to `2038-01-19`. If timestamps outside this range are needed, use `DATETIME(6)` instead.
6. **`BINARY(16)` for UUIDs** — The Java code already converts UUIDs to `byte[16]` via `rawUuidBytes()`. This works with TiDB's `BINARY(16)`.

## Cleanup

```bash
# Stop and remove volumes
docker compose down -v

# Or just stop
docker compose stop
```

## Useful Commands

```bash
# Connect with mysql CLI
mysql -h 127.0.0.1 -P 4000 -u root coordinator

# Check TiDB version
mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT VERSION();"

# Show TiDB configuration
mysql -h 127.0.0.1 -P 4000 -u root -e "SHOW CONFIG;"

# Monitor via dashboard
open http://127.0.0.1:10080