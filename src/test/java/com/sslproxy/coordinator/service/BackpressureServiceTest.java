package com.sslproxy.coordinator.service;

import com.sslproxy.coordinator.config.CoordinatorProperties;
import com.sslproxy.coordinator.fp.DbResult;
import org.apache.camel.CamelContext;
import org.apache.camel.spi.RouteController;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BackpressureServiceTest {

    @Test
    void pendingCountFailurePreservesSuspendedStateWithoutRecovery() throws Exception {
        DatabaseService databaseService = mock(DatabaseService.class);
        CamelContext camelContext = mock(CamelContext.class);
        RouteController routeController = mock(RouteController.class);
        CoordinatorMetricsService metrics = mock(CoordinatorMetricsService.class);
        AdaptivePullController adaptivePull = mock(AdaptivePullController.class);
        when(databaseService.pendingLedgerCount())
                .thenReturn(new DbResult.Ok<>(4_000L))
                .thenReturn(new DbResult.Err<>("coordinator.pending_ledger_count",
                        new IllegalStateException("database unavailable")));
        when(camelContext.getRouteController()).thenReturn(routeController);
        when(adaptivePull.tryLockRouteUpdate("scan-request-consumer", "backpressure_suspend"))
                .thenReturn(true);
        BackpressureService service = new BackpressureService(
                CoordinatorProperties.DEFAULTS, databaseService, camelContext, metrics, adaptivePull);

        service.checkAndAct();
        long failedCheckCount = service.checkAndAct();

        assertTrue(service.isConsumerSuspended());
        assertEquals(service.budget(), failedCheckCount);
        verify(routeController, never()).resumeRoute("scan-request-consumer");
    }
}
