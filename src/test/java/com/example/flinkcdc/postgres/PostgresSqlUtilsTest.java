package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresSqlUtilsTest {
    @Test
    void quotesIdentifiersAndLiterals() {
        assertEquals("\"strange\"\"name\"", PostgresSqlUtils.q("strange\"name"));
        assertEquals("owner''s value", PostgresSqlUtils.sqlLiteral("owner's value"));
    }

    @Test
    void generatesCreateUpsertAndDeleteSql() {
        PostgresSinkConfig config = config();
        TableId id = TableId.tableId("sales-db", "Order");
        Schema schema = Schema.newBuilder()
                .physicalColumn("id", DataTypes.BIGINT().notNull())
                .physicalColumn("select", DataTypes.VARCHAR(100))
                .physicalColumn("payload", DataTypes.BYTES())
                .primaryKey("id")
                .build();

        String create = PostgresSqlUtils.createTable(config, id, schema);
        assertTrue(create.startsWith(
                "CREATE TABLE IF NOT EXISTS \"cdc_sales-db\".\"Order\""));
        assertTrue(create.contains("\"id\" BIGINT NOT NULL"));
        assertTrue(create.contains("\"select\" VARCHAR(100)"));
        assertTrue(create.contains("PRIMARY KEY (\"id\")"));

        assertEquals(
                "INSERT INTO \"cdc_sales-db\".\"Order\" (\"id\",\"select\",\"payload\") "
                        + "VALUES (?,?,?) ON CONFLICT (\"id\") DO UPDATE SET "
                        + "\"select\"=EXCLUDED.\"select\",\"payload\"=EXCLUDED.\"payload\"",
                PostgresSqlUtils.upsert(config, id, schema));
        assertEquals(
                "DELETE FROM \"cdc_sales-db\".\"Order\" WHERE \"id\"=?",
                PostgresSqlUtils.delete(config, id, schema));
    }

    @Test
    void usesDoNothingForPrimaryKeyOnlyTable() {
        TableId id = TableId.tableId("db", "heartbeat");
        Schema schema = Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT().notNull())
                .primaryKey("id")
                .build();

        assertTrue(PostgresSqlUtils.upsert(config(), id, schema).endsWith("DO NOTHING"));
    }

    @Test
    void rejectsTableWithoutPrimaryKey() {
        TableId id = TableId.tableId("db", "events");
        Schema schema = Schema.newBuilder()
                .physicalColumn("value", DataTypes.STRING())
                .build();

        assertThrows(
                IllegalStateException.class,
                () -> PostgresSqlUtils.upsert(config(), id, schema));
    }

    @Test
    void createsHashPartitionsForMergedShardTable() {
        TableId id = TableId.tableId("warehouse", "orders_m");
        Schema schema = Schema.newBuilder()
                .physicalColumn("_pk_id", DataTypes.STRING().notNull())
                .physicalColumn("payload", DataTypes.STRING())
                .primaryKey("_pk_id")
                .partitionKey("_pk_id")
                .option("postgres.partition.strategy", "hash")
                .option("postgres.partition.count", "3")
                .build();

        String create = PostgresSqlUtils.createTable(config(), id, schema);
        List<String> partitions = PostgresSqlUtils.createPartitions(config(), id, schema);

        assertTrue(create.endsWith("PARTITION BY HASH (\"_pk_id\")"));
        assertEquals(3, partitions.size());
        assertTrue(partitions.get(0).contains("PARTITION OF \"cdc_warehouse\".\"orders_m\""));
        assertTrue(partitions.get(0).endsWith("FOR VALUES WITH (MODULUS 3, REMAINDER 0)"));
        assertTrue(partitions.get(2).endsWith("FOR VALUES WITH (MODULUS 3, REMAINDER 2)"));
    }

    @Test
    void createsListPartitionsAndDefaultPartition() {
        TableId id = TableId.tableId("warehouse", "outbound_m");
        Schema schema = Schema.newBuilder()
                .physicalColumn("wh_type", DataTypes.INT().notNull())
                .physicalColumn("_pk_id", DataTypes.STRING().notNull())
                .primaryKey("wh_type", "_pk_id")
                .partitionKey("wh_type")
                .option("postgres.partition.strategy", "list")
                .option("postgres.partition.list.internal", "1|2")
                .option("postgres.partition.list.supplier", "3")
                .option("postgres.partition.default", "true")
                .build();

        List<String> partitions = PostgresSqlUtils.createPartitions(config(), id, schema);

        assertEquals(3, partitions.size());
        assertTrue(partitions.get(0).endsWith("FOR VALUES IN ('1','2')"));
        assertTrue(partitions.get(1).endsWith("FOR VALUES IN ('3')"));
        assertTrue(partitions.get(2).endsWith(" DEFAULT"));
    }

    @Test
    void createsRangePartitionsWithSafeBounds() {
        TableId id = TableId.tableId("sales", "orders_by_month");
        Schema schema = Schema.newBuilder()
                .physicalColumn("created_at", DataTypes.DATE().notNull())
                .physicalColumn("_pk_id", DataTypes.STRING().notNull())
                .primaryKey("created_at", "_pk_id")
                .partitionKey("created_at")
                .option("postgres.partition.strategy", "range")
                .option("postgres.partition.range.p2026_01", "2026-01-01|2026-02-01")
                .option("postgres.partition.range.older", "MINVALUE|2026-01-01")
                .build();

        List<String> partitions = PostgresSqlUtils.createPartitions(config(), id, schema);

        assertEquals(2, partitions.size());
        assertTrue(partitions.get(0).endsWith(
                "FOR VALUES FROM (MINVALUE) TO ('2026-01-01')"));
        assertTrue(partitions.get(1).endsWith(
                "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"));
    }

    @Test
    void requiresPartitionKeyInPrimaryKey() {
        TableId id = TableId.tableId("sales", "bad_partitioned_table");
        Schema schema = Schema.newBuilder()
                .physicalColumn("id", DataTypes.BIGINT().notNull())
                .physicalColumn("created_at", DataTypes.DATE().notNull())
                .primaryKey("id")
                .partitionKey("created_at")
                .build();

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> PostgresSqlUtils.createTable(config(), id, schema));
        assertTrue(exception.getMessage().contains("must include partition key"));
    }

    @Test
    void capsGeneratedPartitionIdentifiersAtPostgresLimit() {
        TableId id = TableId.tableId(
                "warehouse",
                "this_is_a_very_long_merged_table_name_that_would_otherwise_overflow_postgres");

        String first = PostgresSqlUtils.partitionTableName(id, "a very long logical partition");
        String second = PostgresSqlUtils.partitionTableName(id, "a-very-long-logical-partition");

        assertTrue(first.length() <= 63);
        assertTrue(second.length() <= 63);
        assertTrue(!first.equals(second));
    }

    private static PostgresSinkConfig config() {
        Map<String, String> options = new HashMap<>();
        options.put("hostname", "localhost");
        options.put("database-name", "target");
        options.put("username", "user");
        options.put("schema-prefix", "cdc_");
        return PostgresSinkConfig.from(options);
    }
}
