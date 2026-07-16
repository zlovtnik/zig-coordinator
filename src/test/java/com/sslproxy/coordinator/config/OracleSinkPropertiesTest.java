package com.sslproxy.coordinator.config;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OracleSinkPropertiesTest {

    @Test
    void extractsAliasWithoutJdbcConnectionProperties() {
        OracleSinkProperties props = properties("", "jdbc:oracle:thin:@mainerc_high?TNS_ADMIN=/app/wallet");

        assertEquals(Optional.of("mainerc_high"), props.tnsAliasForValidation());
    }

    @Test
    void rejectsDescriptorTargetAfterRemovingProperties() {
        OracleSinkProperties props = properties("", "jdbc:oracle:thin:@host:1521/service?ssl_server_dn_match=true");

        assertEquals(Optional.empty(), props.tnsAliasForValidation());
    }

    private OracleSinkProperties properties(String conn, String jdbcUrl) {
        return new OracleSinkProperties(
                true, true, false, conn, jdbcUrl, "USCIS_APP", "/run/secrets/oracle_password.txt",
                "/app/wallet", 1, 0, 1_000, 1_000, 1_000, 1_000, 0,
                "SELECT 1 FROM DUAL", 5, 3
        );
    }
}
