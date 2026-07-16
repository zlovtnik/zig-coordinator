package com.sslproxy.coordinator.service;

import com.sslproxy.coordinator.fp.RouteAdjustment;
import com.sslproxy.coordinator.config.CoordinatorProperties;
import com.sslproxy.coordinator.fp.DbResult;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.ServiceStatus;
import org.apache.camel.component.kafka.KafkaConfiguration;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.spi.RouteController;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AdaptivePullControllerTest {

    @Test
    void restoresConfigurationAndRouteWhenUpdatedStartFails() throws Exception {
        CamelContext camelContext = mock(CamelContext.class);
        DatabaseService databaseService = mock(DatabaseService.class);
        Route route = mock(Route.class);
        KafkaEndpoint endpoint = mock(KafkaEndpoint.class);
        RouteController routeController = mock(RouteController.class);
        KafkaConfiguration configuration = new KafkaConfiguration();
        configuration.setMaxPollRecords(500);
        when(databaseService.pendingLedgerCount()).thenReturn(new DbResult.Ok<>(4_000L));
        when(camelContext.getRoute("scan-request-consumer")).thenReturn(route);
        when(route.getEndpoint()).thenReturn(endpoint);
        when(endpoint.getConfiguration()).thenReturn(configuration);
        when(camelContext.getRouteController()).thenReturn(routeController);
        when(routeController.getRouteStatus("scan-request-consumer")).thenReturn(ServiceStatus.Started);
        doThrow(new IllegalStateException("updated start failed"))
                .doNothing()
                .doNothing()
                .when(routeController).startRoute("scan-request-consumer");
        AdaptivePullController controller = new AdaptivePullController(
                CoordinatorProperties.DEFAULTS, camelContext, databaseService);

        controller.adjust();
        assertEquals(500, configuration.getMaxPollRecords());

        controller.adjust();
        assertEquals(50, configuration.getMaxPollRecords());
        verify(routeController, times(2)).stopRoute("scan-request-consumer");
        verify(routeController, times(3)).startRoute("scan-request-consumer");
    }

    @Test
    void shrinkNeverGoesBelowMinimumPullRecords() {
        RouteAdjustment result = AdaptivePullController.decideAdjustment(
                10_000,
                1_000,
                500,
                500,
                500,
                10,
                30_000,
                10_000
        );

        RouteAdjustment.Shrink shrink = assertInstanceOf(RouteAdjustment.Shrink.class, result);
        assertTrue(shrink.newMaxPollRecords() >= 50);
    }

    @Test
    void restoreReturnsConfiguredFetchCount() {
        RouteAdjustment result = AdaptivePullController.decideAdjustment(
                100,
                1_000,
                750,
                50,
                50,
                1,
                30_000,
                10_000
        );

        RouteAdjustment.Restore restore = assertInstanceOf(RouteAdjustment.Restore.class, result);
        assertEquals(750, restore.newMaxPollRecords());
    }

    @Test
    void hysteresisSuppressesSmallChangesInsideRestartWindow() {
        RouteAdjustment result = AdaptivePullController.decideAdjustment(
                850,
                1_000,
                500,
                500,
                150,
                200,
                1_000,
                10_000
        );

        RouteAdjustment.NoChange noChange = assertInstanceOf(RouteAdjustment.NoChange.class, result);
        assertEquals("hysteresis", noChange.reason());
    }
}
