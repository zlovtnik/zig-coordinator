package com.sslproxy.coordinator.domain

final case class ScanRequestRecord(
    requestJson: String,
    payloadJson: String,
    payloadSha256: String,
    streamName: String,
    dedupeKey: String,
    observedAt: String
)
