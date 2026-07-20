package com.sslproxy.coordinator.tidb

/** Maps stream names to TiDB sink targets. */
enum TidbSinkTarget(val checksumTag: String):
  case ProxyEvents              extends TidbSinkTarget("proxy.events")
  case ProxyPayloadAudit        extends TidbSinkTarget("proxy.payload_audit")
  case WirelessAuditFrames      extends TidbSinkTarget("wireless.audit")
  case WirelessBandwidth        extends TidbSinkTarget("audit.wireless.bandwidth")
  case WirelessRogueAp          extends TidbSinkTarget("wireless.alert.rogue_ap")
  case WirelessDeauthFlood      extends TidbSinkTarget("wireless.alert.deauth_flood")
  case WirelessSignalAnomaly    extends TidbSinkTarget("wireless.alert.signal_anomaly")
  case WirelessPmfAttack        extends TidbSinkTarget("wireless.alert.pmf_attack")
  case WirelessClientInventory  extends TidbSinkTarget("wireless.client.inventory")
  case WirelessProbeRequests    extends TidbSinkTarget("wireless.probe.flush")

object TidbSinkTarget:
  def fromStreamName(streamName: String): Option[TidbSinkTarget] =
    streamName match
      case "proxy.events"                        => Some(ProxyEvents)
      case "proxy.payload_audit"                 => Some(ProxyPayloadAudit)
      case "wireless.audit"                      => Some(WirelessAuditFrames)
      case "audit.wireless.bandwidth"            => Some(WirelessBandwidth)
      case "wireless.rogue_ap" | "wireless.alert.rogue_ap" => Some(WirelessRogueAp)
      case "wireless.deauth_flood" | "wireless.alert.deauth_flood" => Some(WirelessDeauthFlood)
      case "wireless.signal_anomaly" | "wireless.alert.signal_anomaly" => Some(WirelessSignalAnomaly)
      case "wireless.pmf_attack" | "wireless.alert.pmf_attack" => Some(WirelessPmfAttack)
      case "wireless.client_inventory" | "wireless.client.inventory" => Some(WirelessClientInventory)
      case "wireless.probe_requests" | "wireless.probe.flush" => Some(WirelessProbeRequests)
      case _                                     => None