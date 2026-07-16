package com.sslproxy.coordinator;

import org.apache.camel.CamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import com.sslproxy.coordinator.config.CoordinatorProperties;
import com.sslproxy.coordinator.config.OracleSinkProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableConfigurationProperties({CoordinatorProperties.class, OracleSinkProperties.class})
@EnableScheduling
public class CoordinatorApplication {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorApplication.class);

    private final CamelContext camelContext;

    public CoordinatorApplication(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    public static void main(String[] args) {
        SpringApplication.run(CoordinatorApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        log.info("event=application_ready status=up");
    }

    /**
     * Graceful shutdown handler.
     *
     * On ContextClosedEvent (Spring context close), this suspends all routes
     * to stop accepting new messages, then shuts down the Camel context with
     * a 30-second timeout for in-flight exchanges to complete.
     */
    @EventListener(ContextClosedEvent.class)
    public void onShutdown() {
        log.info("event=shutdown_start status=suspending_routes timeout_seconds=30");

        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        try {
            // Suspend all routes first to stop message flow
            var routeController = camelContext.getRouteController();
            for (var route : camelContext.getRoutes()) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    break;
                }
                String routeId = route.getRouteId();
                if (routeController.getRouteStatus(routeId).isStarted()) {
                    routeController.suspendRoute(routeId, remainingNanos, TimeUnit.NANOSECONDS);
                    log.info("event=route_suspend route={} status=suspended", routeId);
                }
            }

            // Stop Camel context within the remainder of the shared shutdown budget.
            long remainingNanos = Math.max(1L, deadlineNanos - System.nanoTime());
            camelContext.getShutdownStrategy().setTimeUnit(TimeUnit.NANOSECONDS);
            camelContext.getShutdownStrategy().setTimeout(remainingNanos);
            log.info("event=shutdown status=stopping_camel timeout_millis={}",
                    TimeUnit.NANOSECONDS.toMillis(remainingNanos));
            camelContext.shutdown();

            log.info("event=shutdown status=complete");
        } catch (Exception e) {
            log.error("event=shutdown status=error error=\"{}\"", e.getMessage());
        }
    }
}
