package com.sslproxy.coordinator.config

import pureconfig.ConfigReader

final case class WirelessConfig(
    macLookupTopic: String,
    macLookupConsumer: String,
    macLookupReplyTopic: String,
    networksAuthorizedTopic: String,
    networksAuthorizedConsumer: String,
    networksAuthorizedReplyTopic: String,
    probeFlushTopic: String,
    probeFlushConsumer: String,
    consumersCount: Int,
    maxPollRecords: Int,
    dlqSuffix: String
) derives ConfigReader
