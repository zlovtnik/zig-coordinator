package com.sslproxy.coordinator;

import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.ServiceStatus;
import org.apache.camel.spi.RouteController;
import org.apache.camel.spi.ShutdownStrategy;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoordinatorApplicationTest {

    @Test
    void routeSuspensionAndContextShutdownShareOneDeadline() throws Exception {
        CamelContext camelContext = mock(CamelContext.class);
        RouteController routeController = mock(RouteController.class);
        ShutdownStrategy shutdownStrategy = mock(ShutdownStrategy.class);
        Route firstRoute = route("first");
        Route secondRoute = route("second");
        when(camelContext.getRouteController()).thenReturn(routeController);
        when(camelContext.getRoutes()).thenReturn(List.of(firstRoute, secondRoute));
        when(routeController.getRouteStatus("first")).thenReturn(ServiceStatus.Started);
        when(routeController.getRouteStatus("second")).thenReturn(ServiceStatus.Started);
        when(camelContext.getShutdownStrategy()).thenReturn(shutdownStrategy);
        doAnswer(invocation -> {
            Thread.sleep(5);
            return null;
        }).when(routeController).suspendRoute(eq("first"), anyLong(), eq(TimeUnit.NANOSECONDS));
        CoordinatorApplication application = new CoordinatorApplication(camelContext);

        application.onShutdown();

        ArgumentCaptor<Long> routeTimeouts = ArgumentCaptor.forClass(Long.class);
        verify(routeController).suspendRoute(eq("first"), routeTimeouts.capture(), eq(TimeUnit.NANOSECONDS));
        verify(routeController).suspendRoute(eq("second"), routeTimeouts.capture(), eq(TimeUnit.NANOSECONDS));
        ArgumentCaptor<Long> shutdownTimeout = ArgumentCaptor.forClass(Long.class);
        verify(shutdownStrategy).setTimeout(shutdownTimeout.capture());
        assertTrue(routeTimeouts.getAllValues().get(0) <= TimeUnit.SECONDS.toNanos(30));
        assertTrue(routeTimeouts.getAllValues().get(1) < routeTimeouts.getAllValues().get(0));
        assertTrue(shutdownTimeout.getValue() <= routeTimeouts.getAllValues().get(1));
        verify(shutdownStrategy).setTimeUnit(TimeUnit.NANOSECONDS);
        verify(camelContext).shutdown();
    }

    private Route route(String routeId) {
        Route route = mock(Route.class);
        when(route.getRouteId()).thenReturn(routeId);
        return route;
    }
}
