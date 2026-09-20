package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.event.TableId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresSinkConfigTest {
    @Test
    void createsDefaultJdbcUrlAndMapsSourceSchema() {
        Map<String, String> options = requiredOptions();
        options.put("schema-prefix", "cdc_");

        PostgresSinkConfig config = PostgresSinkConfig.from(options);

        assertEquals(
                "jdbc:postgresql://pg.example:5432/cdc_target"
                        + "?reWriteBatchedInserts=true&ApplicationName=flink-cdc-postgres-sink",
                config.jdbcUrl);
        assertEquals(
                "cdc_orders",
                config.targetSchema(TableId.tableId("orders", "customer")));
    }

    @Test
    void fixedSchemaOverridesSourceSchema() {
        Map<String, String> options = requiredOptions();
        options.put("schema-name", "landing");
        options.put("schema-prefix", "ignored_");

        PostgresSinkConfig config = PostgresSinkConfig.from(options);

        assertEquals(
                "landing",
                config.targetSchema(TableId.tableId("source_db", "customer")));
    }

    @Test
    void acceptsJdbcUrlWithoutHostname() {
        Map<String, String> options = requiredOptions();
        options.remove("hostname");
        options.put("jdbc-url", "jdbc:postgresql://pgbouncer:6432/cdc");

        PostgresSinkConfig config = PostgresSinkConfig.from(options);

        assertEquals("jdbc:postgresql://pgbouncer:6432/cdc", config.jdbcUrl);
    }

    @Test
    void validatesRequiredAndSafetyOptions() {
        Map<String, String> missingDatabase = requiredOptions();
        missingDatabase.remove("database-name");
        IllegalArgumentException missing = assertThrows(
                IllegalArgumentException.class,
                () -> PostgresSinkConfig.from(missingDatabase));
        assertTrue(missing.getMessage().contains("database-name"));

        Map<String, String> unsafeNoPk = requiredOptions();
        unsafeNoPk.put("no-primary-key.action", "append");
        assertThrows(
                IllegalArgumentException.class,
                () -> PostgresSinkConfig.from(unsafeNoPk));
    }

    @Test
    void parsesDurations() {
        assertEquals(250L, PostgresSinkConfig.durationMs("250ms"));
        assertEquals(2_000L, PostgresSinkConfig.durationMs("2s"));
        assertEquals(180_000L, PostgresSinkConfig.durationMs("3m"));
        assertEquals(3_600_000L, PostgresSinkConfig.durationMs("1h"));
    }

    @Test
    void validatesPartitionDefaults() {
        Map<String, String> options = requiredOptions();
        options.put("partition.default-strategy", "LIST");
        options.put("partition.default-count", "32");

        PostgresSinkConfig config = PostgresSinkConfig.from(options);

        assertEquals("list", config.partitionDefaultStrategy);
        assertEquals(32, config.partitionDefaultCount);

        options.put("partition.default-strategy", "unknown");
        assertThrows(IllegalArgumentException.class, () -> PostgresSinkConfig.from(options));
        options.put("partition.default-strategy", "hash");
        options.put("partition.default-count", "0");
        assertThrows(IllegalArgumentException.class, () -> PostgresSinkConfig.from(options));
    }

    private static Map<String, String> requiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("hostname", "pg.example");
        options.put("database-name", "cdc_target");
        options.put("username", "flink_cdc");
        return options;
    }
}
