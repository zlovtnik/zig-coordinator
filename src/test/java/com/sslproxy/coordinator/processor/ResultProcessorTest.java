package com.sslproxy.coordinator.processor;

import com.sslproxy.coordinator.config.CoordinatorProperties;
import com.sslproxy.coordinator.fp.DbResult;
import com.sslproxy.coordinator.service.DatabaseService;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ResultProcessorTest {

    @Test
    void buffersUntilFlushTrigger() {
        DatabaseService databaseService = mock(DatabaseService.class);
        when(databaseService.processBatchResults(any())).thenReturn(new DbResult.Ok<>(1));
        ResultProcessor processor = new ResultProcessor(databaseService, CoordinatorProperties.DEFAULTS);

        processor.process(exchange("{\"status\":\"success\"}", null, false));

        verify(databaseService, never()).processBatchResults(any());
        processor.flushPending();
        verify(databaseService).processBatchResults(any());
    }

    @Test
    void commitsLastPollRecordOnlyAfterDurableBatchWrite() {
        DatabaseService databaseService = mock(DatabaseService.class);
        KafkaManualCommit manualCommit = mock(KafkaManualCommit.class);
        when(databaseService.processBatchResults(any())).thenReturn(new DbResult.Ok<>(1));
        ResultProcessor processor = new ResultProcessor(databaseService, CoordinatorProperties.DEFAULTS);

        processor.process(exchange("{\"status\":\"success\"}", manualCommit, true));

        InOrder ordered = inOrder(databaseService, manualCommit);
        ordered.verify(databaseService).processBatchResults(any());
        ordered.verify(manualCommit).commit();
    }

    private Exchange exchange(String body, KafkaManualCommit manualCommit, boolean lastPollRecord) {
        Exchange exchange = mock(Exchange.class);
        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        when(message.getBody(String.class)).thenReturn(body);
        when(message.getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class)).thenReturn(manualCommit);
        when(message.getHeader(KafkaConstants.LAST_POLL_RECORD, Boolean.class)).thenReturn(lastPollRecord);
        return exchange;
    }
}
