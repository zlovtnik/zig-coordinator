-- Coordinator state tables for TiDB
-- Translated from PostgreSQL coordinator schema
-- Uses TiDB-compatible MySQL syntax

CREATE TABLE IF NOT EXISTS sync_cursors (
  stream_name   VARCHAR(255) NOT NULL,
  cursor_value  TEXT NOT NULL,
  updated_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (stream_name)
);

CREATE TABLE IF NOT EXISTS sync_event_tombstones (
  dedupe_key    VARCHAR(255) NOT NULL,
  stream_name   VARCHAR(255) NOT NULL,
  expires_at    DATETIME(6) NOT NULL,
  created_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (dedupe_key, stream_name)
);

CREATE TABLE IF NOT EXISTS sync_events (
  dedupe_key    VARCHAR(255) NOT NULL,
  stream_name   VARCHAR(255) NOT NULL,
  observed_at   DATETIME(6) NOT NULL,
  payload_ref   TEXT,
  payload       JSON,
  payload_sha256 VARCHAR(64),
  status        VARCHAR(32) NOT NULL DEFAULT 'pending',
  attempt_count INT NOT NULL DEFAULT 0,
  last_error    TEXT,
  producer      VARCHAR(128),
  event_kind    VARCHAR(64),
  created_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (dedupe_key, stream_name),
  INDEX idx_sync_events_status (status),
  INDEX idx_sync_events_stream_status (stream_name, status),
  INDEX idx_sync_events_observed (observed_at)
);

CREATE TABLE IF NOT EXISTS sync_jobs (
  job_id        VARCHAR(36) NOT NULL,
  stream_name   VARCHAR(255) NOT NULL,
  status        VARCHAR(32) NOT NULL DEFAULT 'pending',
  attempt_count INT NOT NULL DEFAULT 0,
  created_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  started_at    DATETIME(6),
  finished_at   DATETIME(6),
  PRIMARY KEY (job_id),
  INDEX idx_sync_jobs_status (status),
  INDEX idx_sync_jobs_stream (stream_name)
);

CREATE TABLE IF NOT EXISTS sync_batches (
  batch_id      VARCHAR(36) NOT NULL,
  job_id        VARCHAR(36) NOT NULL,
  batch_no      INT NOT NULL DEFAULT 0,
  payload_ref   TEXT,
  status        VARCHAR(32) NOT NULL DEFAULT 'pending',
  row_count     INT NOT NULL DEFAULT 1,
  checksum      VARCHAR(64),
  attempt_count INT NOT NULL DEFAULT 0,
  last_error    TEXT,
  dedupe_key    VARCHAR(255),
  stream_name   VARCHAR(255),
  cursor_start  VARCHAR(64),
  cursor_end    VARCHAR(64),
  created_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (batch_id),
  INDEX idx_sync_batches_status (status),
  INDEX idx_sync_batches_job (job_id),
  INDEX idx_sync_batches_dedupe (dedupe_key, stream_name),
  INDEX idx_sync_batches_updated (updated_at)
);

CREATE TABLE IF NOT EXISTS sync_errors (
  id            BIGINT AUTO_INCREMENT NOT NULL,
  job_id        VARCHAR(36),
  batch_id      VARCHAR(36),
  error_class   VARCHAR(128),
  error_text    TEXT,
  created_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  INDEX idx_sync_errors_job (job_id),
  INDEX idx_sync_errors_batch (batch_id)
);

CREATE TABLE IF NOT EXISTS devices (
  mac_id        VARCHAR(17) NOT NULL,
  wg_pubkey     TEXT,
  claim_token_hash TEXT,
  display_name  TEXT,
  username      TEXT,
  hostname      TEXT,
  os_hint       TEXT,
  mac_hint      VARCHAR(17) NOT NULL,
  first_seen    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  last_seen     DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  notes         TEXT,
  PRIMARY KEY (mac_id)
);

CREATE TABLE IF NOT EXISTS wireless_authorized_networks (
  id            BIGINT AUTO_INCREMENT NOT NULL,
  ssid          TEXT,
  bssid         TEXT,
  location_id   TEXT,
  label         TEXT,
  enabled       BOOLEAN NOT NULL DEFAULT true,
  notes         TEXT,
  psk_ciphertext TEXT,
  created_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  INDEX idx_wan_enabled (enabled)
);

CREATE TABLE IF NOT EXISTS wireless_clients (
  ssid          TEXT NOT NULL,
  client_mac    TEXT NOT NULL,
  known_bssid   TEXT,
  first_seen    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  last_seen     DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  probe_count   BIGINT NOT NULL DEFAULT 1,
  location_id   TEXT,
  last_probe_batch_id VARCHAR(64),
  PRIMARY KEY (ssid(128), client_mac(17))
);

CREATE TABLE IF NOT EXISTS wireless_shadow_alerts (
  source_mac        VARCHAR(17) NOT NULL,
  first_occurred_at DATETIME(6),
  last_occurred_at  DATETIME(6),
  occurrence_count  INT NOT NULL DEFAULT 1,
  destination_bssid TEXT,
  ssid              TEXT,
  sensor_id         TEXT,
  location_id       TEXT,
  signal_dbm        INT,
  reason            TEXT,
  evidence          JSON,
  resolved_at       DATETIME(6),
  created_at        DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at        DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (source_mac)
);
