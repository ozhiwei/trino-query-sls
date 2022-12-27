package com.github.trino.querysls;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QuerySLSListenerFactoryTest {

    @Test
    void getName() {
        QuerySLSListenerFactory listenerFactory = new QuerySLSListenerFactory();
        assertEquals("trino-query-sls", listenerFactory.getName());
    }

    @Test
    void createWithoutConfigShouldThrowException() {
        // Given
        Map<String, String> configs = new HashMap<>();
        // When
        QuerySLSListenerFactory listenerFactory = new QuerySLSListenerFactory();
    }
}