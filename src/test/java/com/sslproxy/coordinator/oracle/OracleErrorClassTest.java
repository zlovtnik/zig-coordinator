package com.sslproxy.coordinator.oracle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.Locale;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OracleErrorClassTest {

    @Test
    void classifiesJdbcTransientMetadataAcrossCauseChain() {
        assertEquals(OracleErrorClass.RETRYABLE,
                OracleErrorClass.classify(new RuntimeException(new SQLRecoverableException("socket closed"))));
        assertEquals(OracleErrorClass.RETRYABLE,
                OracleErrorClass.classify(new SQLTransientException("busy")));
        assertEquals(OracleErrorClass.RETRYABLE,
                OracleErrorClass.classify(new SQLException("network", "08006", 0)));
        assertEquals(OracleErrorClass.RETRYABLE,
                OracleErrorClass.classify(new SQLException("io", null, 17002)));
    }

    @Test
    @ResourceLock(Resources.LOCALE)
    void classifyIsStableUnderTurkishLocale() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr"));

            assertEquals(OracleErrorClass.RETRYABLE,
                    OracleErrorClass.classify(new java.sql.SQLException("DPI-1067 connection reset")));
        } finally {
            Locale.setDefault(original);
        }
    }
}
