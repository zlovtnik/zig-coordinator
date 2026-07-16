package com.sslproxy.coordinator.route;

import com.sslproxy.coordinator.config.CoordinatorProperties;
import com.sslproxy.coordinator.processor.ResultProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Consumes Oracle results from sync.oracle.result topic.
 * Replaces sync_handlers.zig handleResults() and the CLI-based
 * service_redpanda.zig pullResultBatch().
 *
 * Each result message is accumulated into batches by ResultProcessor
 * and flushed to the DB via coordinator.process_batch_results().
 */
@Component
public class OracleResultRoute extends RouteBuilder {

    private static final Logger log = LoggerFactory.getLogger(OracleResultRoute.class);

    private final CoordinatorProperties props;
    private final ResultProcessor resultProcessor;

    public OracleResultRoute(CoordinatorProperties props,
                             ResultProcessor resultProcessor) {
        this.props = props;
        this.resultProcessor = resultProcessor;
    }

    @Override
    public void configure() {
        onException(IllegalStateException.class)
                .onWhen(exchange -> fromRoute(exchange, "oracle-result-consumer"))
                .maximumRedeliveries(0)
                .handled(false);

        onException(Exception.class)
                .onWhen(exchange -> fromRoute(exchange, "oracle-result-consumer"))
                .maximumRedeliveries("{{coordinator.result-route-max-retries:3}}")
                .redeliveryDelay(500)
                .useOriginalMessage()
                .handled(true)
                .process(exchange -> logRouteFailure(exchange, props.resultTopic() + ".dlq"))
                .to("kafka:{{coordinator.result-topic}}.dlq")
                .process(this::commitOffset);

        onException(Exception.class)
                .onWhen(exchange -> fromRoute(exchange, "oracle-result-flush-timer"))
                .maximumRedeliveries("{{coordinator.result-route-max-retries:3}}")
                .redeliveryDelay(500)
                .handled(true)
                .process(this::logTimerFailure);

        // Consume from sync.oracle.result with the zig-coordinator-result consumer group
        from("kafka:{{coordinator.result-topic}}"
                + "?groupId={{coordinator.result-consumer}}"
                + "&autoOffsetReset=earliest"
                + "&maxPollRecords={{coordinator.result-fetch-count}}"
                + "&consumersCount=1"
                + "&allowManualCommit=true"
                + "&autoCommitEnable=false"
                + "&breakOnFirstError=true")
        .routeId("oracle-result-consumer")
        .process(exchange -> {
            if (log.isTraceEnabled()) {
                String body = exchange.getIn().getBody(String.class);
                int payloadBytes = body == null ? 0 : body.getBytes(StandardCharsets.UTF_8).length;
                log.atTrace()
                        .addKeyValue("event", "batch_result_ingest")
                        .addKeyValue("status", "received")
                        .addKeyValue("route", "oracle-result-consumer")
                        .addKeyValue("topic", props.resultTopic())
                        .addKeyValue("consumer_group", props.resultConsumer())
                        .addKeyValue("payload_bytes", payloadBytes)
                        .log("oracle result received");
            }
        })
        .process(resultProcessor);

        // Timer-based flush for partial batches - ensures results don't sit in the
        // accumulator indefinitely when Kafka delivers fewer messages than the batch size
        from("timer:result-flush?period=1000&daemon=true")
                .routeId("oracle-result-flush-timer")
                .bean(resultProcessor, "flushPending")
                .log(LoggingLevel.TRACE, "event=result_flush_timer status=tick");
    }

    private void logRouteFailure(Exchange exchange, String dlqTopic) {
        Exception error = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        log.atError()
                .addKeyValue("event", "batch_result_ingest")
                .addKeyValue("status", "dlq")
                .addKeyValue("dlq_topic", dlqTopic)
                .addKeyValue("error", sanitize(error == null ? "" : error.getMessage()))
                .log("oracle result routed to DLQ");
    }

    private void logTimerFailure(Exchange exchange) {
        Exception error = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        log.atError()
                .addKeyValue("event", "batch_result_flush")
                .addKeyValue("status", "failed")
                .addKeyValue("error", sanitize(error == null ? "" : error.getMessage()))
                .log("oracle result timer flush failed; records remain buffered");
    }

    private boolean fromRoute(Exchange exchange, String routeId) {
        return routeId.equals(exchange.getFromRouteId());
    }

    private void commitOffset(Exchange exchange) {
        KafkaManualCommit manualCommit = exchange.getIn()
                .getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (manualCommit != null) {
            manualCommit.commit();
        }
    }

    private String sanitize(String message) {
        if (message == null || message.isBlank()) {
            return "";
        }
        return message.replace('\n', ' ').replace('\r', ' ');
    }
}
