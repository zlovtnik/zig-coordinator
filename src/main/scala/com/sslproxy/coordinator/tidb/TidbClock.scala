package com.sslproxy.coordinator.tidb

import java.time.{OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

/** Clock abstraction. */
object TidbClock:

  def nowRfc3339: String =
    OffsetDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)