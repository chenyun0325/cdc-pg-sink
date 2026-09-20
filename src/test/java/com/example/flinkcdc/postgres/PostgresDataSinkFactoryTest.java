package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.factories.Factory;
import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresDataSinkFactoryTest {
    @Test
    void isDiscoverableThroughFactorySpi() {
        boolean found = ServiceLoader.load(Factory.class).stream()
                .map(ServiceLoader.Provider::get)
                .anyMatch(factory -> factory instanceof PostgresDataSinkFactory
                        && "postgres".equals(factory.identifier()));

        assertTrue(found, "PostgreSQL sink factory should be discoverable through ServiceLoader");
    }
}
