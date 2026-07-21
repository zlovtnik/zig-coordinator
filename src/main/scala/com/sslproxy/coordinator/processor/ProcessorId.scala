package com.sslproxy.coordinator.processor

enum ProcessorFamily(val value: String):
  case Sync extends ProcessorFamily("sync")
  case Wireless extends ProcessorFamily("wireless")
  case Embedding extends ProcessorFamily("embedding")
  case SearchProjection extends ProcessorFamily("search_projection")
  case Console extends ProcessorFamily("console")
  case Maintenance extends ProcessorFamily("maintenance")

enum ProcessorId(val value: String, val family: ProcessorFamily):
  case SyncScanIngestion extends ProcessorId("sync-scan-ingestion", ProcessorFamily.Sync)
  case SyncJobPlanner extends ProcessorId("sync-job-planner", ProcessorFamily.Sync)
  case SyncBacklogRecovery extends ProcessorId("sync-backlog-recovery", ProcessorFamily.Sync)
  case SyncLoadDispatch extends ProcessorId("sync-load-dispatch", ProcessorFamily.Sync)
  case SyncLoadConsumer extends ProcessorId("sync-load-consumer", ProcessorFamily.Sync)
  case SyncResultConsumer extends ProcessorId("sync-result-consumer", ProcessorFamily.Sync)
  case SyncOutboxPublisher extends ProcessorId("sync-outbox-publisher", ProcessorFamily.Sync)
  case WirelessFrameNormalizer extends ProcessorId("wireless-frame-normalizer", ProcessorFamily.Wireless)
  case WirelessInventoryProjector extends ProcessorId("wireless-inventory-projector", ProcessorFamily.Wireless)
  case WirelessIdentityProjector extends ProcessorId("wireless-identity-projector", ProcessorFamily.Wireless)
  case EmbeddingPreparer extends ProcessorId("embedding-preparer", ProcessorFamily.Embedding)
  case EmbeddingCompleter extends ProcessorId("embedding-completer", ProcessorFamily.Embedding)
  case EmbeddingLeaseRecovery extends ProcessorId("embedding-lease-recovery", ProcessorFamily.Embedding)
  case EmbeddingTextBuilder extends ProcessorId("embedding-text-builder", ProcessorFamily.Embedding)
  case BehaviorProjector extends ProcessorId("behavior-projector", ProcessorFamily.SearchProjection)
  case TimingProjector extends ProcessorId("timing-projector", ProcessorFamily.SearchProjection)
  case BaselineProjector extends ProcessorId("baseline-projector", ProcessorFamily.SearchProjection)
  case SequenceProjector extends ProcessorId("sequence-projector", ProcessorFamily.SearchProjection)
  case GraphProjector extends ProcessorId("graph-projector", ProcessorFamily.SearchProjection)
  case SimilarityProjector extends ProcessorId("similarity-projector", ProcessorFamily.SearchProjection)
  case ClusteringProjector extends ProcessorId("clustering-projector", ProcessorFamily.SearchProjection)
  case DnsAlertProjector extends ProcessorId("dns-alert-projector", ProcessorFamily.SearchProjection)
  case RfAlertProjector extends ProcessorId("rf-alert-projector", ProcessorFamily.SearchProjection)
  case RiskProjector extends ProcessorId("risk-projector", ProcessorFamily.SearchProjection)
  case ConsoleCommandSubscriber extends ProcessorId("console-command-subscriber", ProcessorFamily.Console)
  case ConsoleHeartbeatProjector extends ProcessorId("console-heartbeat-projector", ProcessorFamily.Console)
  case ConsoleHeatmapProjector extends ProcessorId("console-heatmap-projector", ProcessorFamily.Console)
  case ConsoleActionCableFanout extends ProcessorId("console-actioncable-fanout", ProcessorFamily.Console)
  case EventRetention extends ProcessorId("event-retention", ProcessorFamily.Maintenance)
  case SearchRetention extends ProcessorId("search-retention", ProcessorFamily.Maintenance)
  case StaleWorkerCleanup extends ProcessorId("stale-worker-cleanup", ProcessorFamily.Maintenance)
  case ScheduledReconciliation extends ProcessorId("scheduled-reconciliation", ProcessorFamily.Maintenance)

object ProcessorId:
  private val byValue: Map[String, ProcessorId] =
    ProcessorId.values.iterator.map(id => id.value -> id).toMap

  def fromString(value: String): Either[String, ProcessorId] =
    byValue.get(value).toRight(s"unknown processor id: $value")

  val all: List[ProcessorId] = ProcessorId.values.toList

final case class ProcessorContract(
    id: ProcessorId,
    input: String,
    output: String,
    dedupeKey: String,
    leaseScope: String,
    terminalBehavior: String,
    reconciliation: String
)

/** Stable, machine-readable workload inventory. Detailed legacy-source parity
  * remains in the repository-level parity matrix; this catalog is the runtime
  * side of that exact-one mapping.
  */
object ProcessorCatalog:
  val contracts: List[ProcessorContract] = List(
    contract(ProcessorId.SyncScanIngestion, "sync.scan.request", "sync_events,ingestion_evidence", "group/topic/partition/offset", "kafka partition", "park/DLQ invalid records", "bounded offset audit"),
    contract(ProcessorId.SyncJobPlanner, "sync_events", "sync_jobs,sync_batches,outbox_events", "stream_name/dedupe_key", "stream", "park exhausted work", "orphan event scan"),
    contract(ProcessorId.SyncBacklogRecovery, "expired sync leases", "sync_jobs,sync_batches", "job_id/batch_id", "batch", "fail exhausted batch", "lease expiry scan"),
    contract(ProcessorId.SyncLoadDispatch, "outbox_events", "sync.oracle.load", "destination_topic/message_key", "outbox row", "park exhausted publication", "published-attempt audit"),
    contract(ProcessorId.SyncLoadConsumer, "sync.oracle.load", "domain tables,outbox_events", "batch_id/attempt", "kafka partition", "sync result failure", "batch checksum"),
    contract(ProcessorId.SyncResultConsumer, "sync.oracle.result", "sync_jobs,sync_batches,sync_cursors", "batch_id/attempt", "kafka partition", "terminal job failure", "batch/cursor scan"),
    contract(ProcessorId.SyncOutboxPublisher, "outbox_events", "Redpanda", "destination_topic/message_key", "outbox row", "park exhausted publication", "publish-attempt audit"),
    contract(ProcessorId.WirelessFrameNormalizer, "wireless.audit", "wireless frames/sensors", "event_id", "wireless event", "park invalid frame", "source/frame checksum"),
    contract(ProcessorId.WirelessInventoryProjector, "wireless frames", "device/client inventory", "device/window", "inventory key", "record reconciliation finding", "inventory rebuild"),
    contract(ProcessorId.WirelessIdentityProjector, "inventory/probes", "identity projections", "identity/source", "identity key", "record reconciliation finding", "identity rebuild"),
    contract(ProcessorId.EmbeddingPreparer, "search sources", "embedding jobs", "kind/source/checksum", "embedding job", "park invalid source", "missing-vector scan"),
    contract(ProcessorId.EmbeddingCompleter, "embedding results", "vector tables", "job_id/model", "embedding job", "park dimension/model mismatch", "source/vector checksum"),
    contract(ProcessorId.EmbeddingLeaseRecovery, "expired embedding leases", "embedding jobs", "job_id/attempt", "embedding job", "fail exhausted job", "lease expiry scan"),
    contract(ProcessorId.EmbeddingTextBuilder, "normalized domain rows", "search documents/tokens", "source/checksum", "source row", "park malformed text input", "document checksum"),
    contract(ProcessorId.BehaviorProjector, "wireless/proxy events", "behavior snapshots", "subject/window/version", "projection key", "record reconciliation finding", "window rebuild"),
    contract(ProcessorId.TimingProjector, "events", "timing projections", "subject/window/version", "projection key", "record reconciliation finding", "window rebuild"),
    contract(ProcessorId.BaselineProjector, "timing/behavior", "baseline projections", "subject/window/version", "projection key", "record reconciliation finding", "baseline rebuild"),
    contract(ProcessorId.SequenceProjector, "ordered events", "sequence transitions", "subject/from/to/window", "projection key", "record reconciliation finding", "transition rebuild"),
    contract(ProcessorId.GraphProjector, "documents/identities", "graph nodes/edges", "node-or-edge/version", "graph key", "record reconciliation finding", "graph rebuild"),
    contract(ProcessorId.SimilarityProjector, "vectors", "similarity projections", "left/right/model", "pair key", "record reconciliation finding", "similarity rebuild"),
    contract(ProcessorId.ClusteringProjector, "similarities", "identity clusters", "member/model/version", "cluster key", "record reconciliation finding", "cluster rebuild"),
    contract(ProcessorId.DnsAlertProjector, "proxy DNS events", "threat signals", "rule/subject/window", "alert key", "park invalid evidence", "rule replay"),
    contract(ProcessorId.RfAlertProjector, "wireless events", "wireless alerts/threat signals", "rule/subject/window", "alert key", "park invalid evidence", "rule replay"),
    contract(ProcessorId.RiskProjector, "alerts/behavior", "risk projections", "subject/model/version", "risk key", "record reconciliation finding", "risk rebuild"),
    contract(ProcessorId.ConsoleCommandSubscriber, "integration_console.console_commands", "core state/command results", "command_id", "command", "rejected/failed acknowledgement", "unacknowledged command scan"),
    contract(ProcessorId.ConsoleHeartbeatProjector, "runtime health", "console heartbeat", "component/time bucket", "component", "record unavailable component", "stale heartbeat scan"),
    contract(ProcessorId.ConsoleHeatmapProjector, "wireless observations", "console heatmap", "location/time bucket", "heatmap bucket", "record reconciliation finding", "bucket rebuild"),
    contract(ProcessorId.ConsoleActionCableFanout, "console outbox", "Redis ActionCable", "event/channel", "outbox row", "retry without authoritative loss", "outbox audit"),
    contract(ProcessorId.EventRetention, "expired core events/tombstones", "retention_runs", "policy/cutoff", "retention policy", "record failed run", "30d/45d boundary scan"),
    contract(ProcessorId.SearchRetention, "expired search rows", "retention_runs", "policy/cutoff", "retention policy", "record failed run", "search expiry scan"),
    contract(ProcessorId.StaleWorkerCleanup, "expired worker leases", "jobs/leases", "work_id/fence", "work item", "park exhausted work", "lease scan"),
    contract(ProcessorId.ScheduledReconciliation, "domain/projection state", "reconciliation_findings", "processor/entity/version", "processor shard", "persist unresolved finding", "deterministic diff/repair")
  )

  private def contract(
      id: ProcessorId,
      input: String,
      output: String,
      dedupeKey: String,
      leaseScope: String,
      terminalBehavior: String,
      reconciliation: String
  ): ProcessorContract =
    ProcessorContract(id, input, output, dedupeKey, leaseScope, terminalBehavior, reconciliation)

