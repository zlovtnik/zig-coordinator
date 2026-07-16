package com.sslproxy.coordinator.service;

import com.sslproxy.coordinator.config.OracleSinkProperties;
import com.sslproxy.coordinator.fp.DbResult;
import com.sslproxy.coordinator.oracle.OracleConnectionFactory;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HealthCheckServiceTest {

    @Test
    void checksRedpandaBeforeReportingHealthy() throws Exception {
        DatabaseService databaseService = mock(DatabaseService.class);
        RedpandaLagMetricsService redpanda = mock(RedpandaLagMetricsService.class);
        OracleSinkProperties oracleProperties = mock(OracleSinkProperties.class);
        when(databaseService.checkConnectivity()).thenReturn(new DbResult.Ok<>(null));
        when(databaseService.ensureAllCursors()).thenReturn(new DbResult.Ok<>(null));
        when(oracleProperties.enabled()).thenReturn(false);
        HealthCheckService service = new HealthCheckService(
                databaseService, redpanda, oracleProperties, mock(OracleConnectionFactory.class));

        service.checkHealth();

        verify(redpanda).checkConnectivity();
    }
}
