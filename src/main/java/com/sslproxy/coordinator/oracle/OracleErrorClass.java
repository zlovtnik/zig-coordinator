package com.sslproxy.coordinator.oracle;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;

public enum OracleErrorClass {
    RETRYABLE("retryable"),
    PERMANENT("permanent");

    private final String wireValue;

    OracleErrorClass(String wireValue) {
        this.wireValue = wireValue;
    }

    public String wireValue() {
        return wireValue;
    }

    public static OracleErrorClass classify(Throwable failure) {
        if (failure == null) {
            return PERMANENT;
        }

        ArrayDeque<Throwable> failures = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        failures.add(failure);
        while (!failures.isEmpty()) {
            Throwable current = failures.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current instanceof SQLRecoverableException || current instanceof SQLTransientException) {
                return RETRYABLE;
            }
            if (current instanceof SQLException sqlException) {
                if (isRetryableSqlState(sqlException.getSQLState())
                        || isRetryableVendorCode(sqlException.getErrorCode())) {
                    return RETRYABLE;
                }
                if (sqlException.getNextException() != null) {
                    failures.addLast(sqlException.getNextException());
                }
            }
            if (isRetryableMessage(current.getMessage())) {
                return RETRYABLE;
            }
            if (current.getCause() != null && current.getCause() != current) {
                failures.addLast(current.getCause());
            }
        }
        return PERMANENT;
    }

    private static boolean isRetryableSqlState(String sqlState) {
        if (sqlState == null) {
            return false;
        }
        String normalized = sqlState.toUpperCase(Locale.ROOT);
        return normalized.startsWith("08")
                || normalized.startsWith("40")
                || normalized.equals("HYT00")
                || normalized.equals("HYT01");
    }

    private static boolean isRetryableVendorCode(int errorCode) {
        return switch (Math.abs(errorCode)) {
            case 54, 60, 1013, 3113, 3114, 3135, 3136, 12514, 12541, 12545, 17002, 17410 -> true;
            default -> false;
        };
    }

    private static boolean isRetryableMessage(String message) {
        String normalized = message == null ? "" : message.toLowerCase(Locale.ROOT);
        return normalized.contains("timeout")
                || normalized.contains("dpi-1067")
                || normalized.contains("ora-03114")
                || normalized.contains("ora-03136")
                || normalized.contains("ora-3136")
                || normalized.contains("temporarily unavailable")
                || normalized.contains("connection reset")
                || normalized.contains("deadlock");
    }
}
