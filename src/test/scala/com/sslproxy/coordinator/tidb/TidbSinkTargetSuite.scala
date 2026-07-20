package com.sslproxy.coordinator.tidb

import munit.*

class TidbSinkTargetSuite extends FunSuite:

  test("fromStreamName returns ProxyEvents for proxy.events"):
    assertEquals(TidbSinkTarget.fromStreamName("proxy.events"), Some(TidbSinkTarget.ProxyEvents))

  test("fromStreamName returns WirelessAuditFrames for wireless.audit"):
    assertEquals(TidbSinkTarget.fromStreamName("wireless.audit"), Some(TidbSinkTarget.WirelessAuditFrames))

  test("fromStreamName returns None for unknown stream"):
    assertEquals(TidbSinkTarget.fromStreamName("unknown.stream"), None)

  test("fromStreamName accepts legacy aliases"):
    assertEquals(TidbSinkTarget.fromStreamName("wireless.rogue_ap"), Some(TidbSinkTarget.WirelessRogueAp))
    assertEquals(TidbSinkTarget.fromStreamName("wireless.deauth_flood"), Some(TidbSinkTarget.WirelessDeauthFlood))
    assertEquals(TidbSinkTarget.fromStreamName("wireless.signal_anomaly"), Some(TidbSinkTarget.WirelessSignalAnomaly))
    assertEquals(TidbSinkTarget.fromStreamName("wireless.pmf_attack"), Some(TidbSinkTarget.WirelessPmfAttack))
    assertEquals(TidbSinkTarget.fromStreamName("wireless.client_inventory"), Some(TidbSinkTarget.WirelessClientInventory))
    assertEquals(TidbSinkTarget.fromStreamName("wireless.probe_requests"), Some(TidbSinkTarget.WirelessProbeRequests))

  test("checksumTag is non-empty for all targets"):
    for target <- TidbSinkTarget.values do
      assert(target.checksumTag.nonEmpty, s"${target} has empty checksumTag")
