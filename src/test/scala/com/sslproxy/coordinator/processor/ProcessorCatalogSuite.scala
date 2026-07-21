package com.sslproxy.coordinator.processor

import munit.FunSuite

class ProcessorCatalogSuite extends FunSuite:
  test("catalog covers every runtime processor exactly once") {
    val ids = ProcessorCatalog.contracts.map(_.id)
    assertEquals(ids.toSet, ProcessorId.all.toSet)
    assertEquals(ids.distinct.size, ids.size)
  }

  test("processor identifiers are stable and kebab-cased") {
    ProcessorId.all.foreach { id =>
      assert(id.value.matches("^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$"), id.value)
      assertEquals(ProcessorId.fromString(id.value), Right(id))
    }
  }

  test("every processor declares its consistency contract") {
    ProcessorCatalog.contracts.foreach { contract =>
      assert(contract.input.nonEmpty, contract.id.value)
      assert(contract.output.nonEmpty, contract.id.value)
      assert(contract.dedupeKey.nonEmpty, contract.id.value)
      assert(contract.leaseScope.nonEmpty, contract.id.value)
      assert(contract.terminalBehavior.nonEmpty, contract.id.value)
      assert(contract.reconciliation.nonEmpty, contract.id.value)
    }
  }

