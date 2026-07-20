package com.sslproxy.coordinator.tidb

import java.sql.{SQLException, SQLRecoverableException, SQLTransientException}
import munit.*

class TidbErrorClassSuite extends FunSuite:

  test("classify null as Permanent"):
    assertEquals(TidbErrorClass.classify(null), TidbErrorClass.Permanent)

  test("classify IllegalArgumentException as Permanent"):
    assertEquals(TidbErrorClass.classify(IllegalArgumentException("bad")), TidbErrorClass.Permanent)

  test("classify SQLRecoverableException as Retryable"):
    assertEquals(TidbErrorClass.classify(SQLRecoverableException()), TidbErrorClass.Retryable)

  test("classify SQLTransientException as Retryable"):
    assertEquals(TidbErrorClass.classify(SQLTransientException()), TidbErrorClass.Retryable)

  test("classify MySQL connection error 2006 as Retryable"):
    assertEquals(TidbErrorClass.classify(SQLException("MySQL server has gone away", "08S01", 2006)), TidbErrorClass.Retryable)

  test("classify MySQL deadlock 1213 as Retryable"):
    assertEquals(TidbErrorClass.classify(SQLException("Deadlock found", "40001", 1213)), TidbErrorClass.Retryable)

  test("classify TiDB write conflict 8002 as Retryable"):
    assertEquals(TidbErrorClass.classify(SQLException("Write conflict", "40001", 8002)), TidbErrorClass.Retryable)

  test("classify TiDB region unavailable 9007 as Retryable"):
    assertEquals(TidbErrorClass.classify(SQLException("Region unavailable", "HY000", 9007)), TidbErrorClass.Retryable)

  test("classify non-retryable SQL error as Permanent"):
    assertEquals(TidbErrorClass.classify(SQLException("Syntax error", "42000", 1064)), TidbErrorClass.Permanent)

  test("classify chained exception with recoverable at root as Retryable"):
    val root = SQLRecoverableException("connection lost")
    val outer = RuntimeException("wrapping", root)
    assertEquals(TidbErrorClass.classify(outer), TidbErrorClass.Retryable)

  test("classify message containing timeout as Retryable"):
    assertEquals(TidbErrorClass.classify(RuntimeException("query timeout expired")), TidbErrorClass.Retryable)

  test("classify message containing write conflict as Retryable"):
    assertEquals(TidbErrorClass.classify(RuntimeException("Write conflict, txn restarted")), TidbErrorClass.Retryable)
