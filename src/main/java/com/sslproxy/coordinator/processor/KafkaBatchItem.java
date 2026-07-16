package com.sslproxy.coordinator.processor;

import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;

record KafkaBatchItem<T>(T value, KafkaManualCommit manualCommit) {

    static <T> KafkaBatchItem<T> from(Exchange exchange, T value) {
        KafkaManualCommit manualCommit = exchange.getIn()
                .getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        return new KafkaBatchItem<>(value, manualCommit);
    }

    static boolean isLastPollRecord(Exchange exchange) {
        return Boolean.TRUE.equals(exchange.getIn()
                .getHeader(KafkaConstants.LAST_POLL_RECORD, Boolean.class));
    }

    static void commitExchange(Exchange exchange) {
        KafkaManualCommit manualCommit = exchange.getIn()
                .getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (manualCommit != null) {
            manualCommit.commit();
        }
    }

    boolean requiresManualCommit() {
        return manualCommit != null;
    }

    void commit() {
        if (manualCommit != null) {
            manualCommit.commit();
        }
    }
}
