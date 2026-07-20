# AGENTS.md

## Scope
This file governs `/Users/rcs/git/ssl-proxy/services/zig-coordinator`.

## Project Shape
- The directory name is legacy. The service is Scala 3 with Cats Effect 3,
  FS2, Doobie, http4s, fs2-kafka, Circe, and sbt build.
- `src/main/scala/com/sslproxy/coordinator/config/` owns pureconfig-backed
  configuration with environment variable overrides.
- `tidb/` owns TiDB connection setup, transforms, checksums, error classification,
  schema preflight, and sink behavior.
- `postgres/` owns Postgres cursoring, batch dispatch, and result processing.
- `kafka/` owns FS2 Kafka consumer/producer streams for load and result topics.
- `cron/` owns the periodic ingest/batch/dispatch scheduler loop.
- `dispatch/` owns Kafka batch dispatch from Postgres backlog.
- `http/` owns the http4s health endpoint.
- `src/test/scala/` includes MUnit and MUnit Cats Effect tests.

## Guardrails
- Keep this service the only TiDB/MySQL owner. TiDB wiring should not migrate
  into Rust proxy, sensor, search, or shared crates.
- Preserve the locked topics and consumer roles for `sync.scan.request`,
  `sync.oracle.load`, and `sync.oracle.result`.
- Keep coordinator operations retry-safe and idempotent: cursor advancement,
  batch leasing, dispatch release/failure, backlog handling, and result
  processing must tolerate duplicate delivery.
- Keep PostgreSQL function call signatures aligned with root `sql/functions/*`
  and `sql/postgres.source.sql`; update contract tests when signatures change.
- Do not commit sbt caches, IDE state, `.omx/` output, or generated local
  runtime files.

## Commands
- Run tests from this directory: `sbt test`.
- Build: `sbt assembly`.
- Root broad test target also runs coordinator tests: `make test`.

## Verification
- Run focused sbt tests for changed packages when practical, then
  `sbt test` for coordinator-wide changes.
- For TiDB sink changes, cover schema preflight failure modes, retry
  classification, transform output, and disabled-sink behavior.
