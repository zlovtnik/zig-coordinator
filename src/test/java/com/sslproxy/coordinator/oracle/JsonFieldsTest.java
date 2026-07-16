package com.sslproxy.coordinator.oracle;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonFieldsTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void optionalLongRejectsFractionalAndOutOfRangeNumbers() throws Exception {
        var row = objectMapper.readTree("""
                {"integral":42,"fractional":1.5,"too_large":9223372036854775808}
                """);

        assertEquals(Optional.of(42L), JsonFields.optionalLong(row, "integral"));
        assertEquals(Optional.empty(), JsonFields.optionalLong(row, "fractional"));
        assertEquals(Optional.empty(), JsonFields.optionalLong(row, "too_large"));
    }
}
