package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.zip.CRC32;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

final class PostgresSqlUtils {
    private PostgresSqlUtils() {}

    static String q(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("PostgreSQL identifier must not be empty.");
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    static String schema(PostgresSinkConfig config, TableId id) {
        return config.targetSchema(id);
    }

    static String tableRef(PostgresSinkConfig config, TableId id) {
        return q(schema(config, id)) + "." + q(id.getTableName());
    }

    static String tableRef(PostgresSinkConfig config, TableId id, String tableName) {
        return q(schema(config, id)) + "." + q(tableName);
    }

    static String createTable(PostgresSinkConfig config, TableId id, Schema schema) {
        Optional<PostgresPartitionSpec> partitionSpec =
                PostgresPartitionSpec.from(config, id, schema);
        List<String> definitions = new ArrayList<>();
        Set<String> primaryKeys = new HashSet<>(schema.primaryKeys());
        for (Column column : schema.getColumns()) {
            StringBuilder definition = new StringBuilder()
                    .append(q(column.getName())).append(' ')
                    .append(PostgresTypeMapper.toPostgres(column.getType()));
            if (!column.getType().isNullable() || primaryKeys.contains(column.getName())) {
                definition.append(" NOT NULL");
            }
            definitions.add(definition.toString());
        }
        if (!schema.primaryKeys().isEmpty()) {
            definitions.add("PRIMARY KEY ("
                    + schema.primaryKeys().stream()
                            .map(PostgresSqlUtils::q)
                            .collect(Collectors.joining(","))
                    + ")");
        }
        String sql = "CREATE TABLE IF NOT EXISTS " + tableRef(config, id) + " (\n  "
                + String.join(",\n  ", definitions) + "\n)";
        if (partitionSpec.isPresent()) {
            PostgresPartitionSpec spec = partitionSpec.get();
            sql += " PARTITION BY " + spec.strategy.name() + " ("
                    + spec.columns.stream()
                            .map(PostgresSqlUtils::q)
                            .collect(Collectors.joining(","))
                    + ")";
        }
        return sql;
    }

    static List<String> createPartitions(
            PostgresSinkConfig config, TableId id, Schema schema) {
        Optional<PostgresPartitionSpec> optionalSpec =
                PostgresPartitionSpec.from(config, id, schema);
        if (optionalSpec.isEmpty()) {
            return List.of();
        }

        PostgresPartitionSpec spec = optionalSpec.get();
        List<String> sql = new ArrayList<>();
        if (spec.strategy == PostgresPartitionSpec.Strategy.HASH) {
            for (int remainder = 0; remainder < spec.hashPartitionCount; remainder++) {
                String logicalName = String.format(
                        Locale.ROOT, "h%0" + decimalDigits(spec.hashPartitionCount - 1) + "d",
                        remainder);
                sql.add(createPartitionPrefix(config, id, logicalName)
                        + " FOR VALUES WITH (MODULUS " + spec.hashPartitionCount
                        + ", REMAINDER " + remainder + ")");
            }
            return sql;
        }

        for (PostgresPartitionSpec.NamedPartition partition : spec.partitions) {
            String prefix = createPartitionPrefix(config, id, partition.name);
            if (spec.strategy == PostgresPartitionSpec.Strategy.LIST) {
                sql.add(prefix + " FOR VALUES IN ("
                        + partition.values.stream()
                                .map(PostgresSqlUtils::listPartitionValue)
                                .collect(Collectors.joining(","))
                        + ")");
            } else {
                sql.add(prefix + " FOR VALUES FROM ("
                        + rangePartitionValue(partition.values.get(0))
                        + ") TO (" + rangePartitionValue(partition.values.get(1)) + ")");
            }
        }
        if (spec.createDefaultPartition) {
            sql.add(createPartitionPrefix(config, id, "default") + " DEFAULT");
        }
        return sql;
    }

    private static String createPartitionPrefix(
            PostgresSinkConfig config, TableId id, String logicalName) {
        return "CREATE TABLE IF NOT EXISTS "
                + tableRef(config, id, partitionTableName(id, logicalName))
                + " PARTITION OF " + tableRef(config, id);
    }

    static String partitionTableName(TableId id, String logicalName) {
        String parent = asciiIdentifier(id.getTableName());
        String suffix = asciiIdentifier(logicalName);
        if (suffix.length() > 16) {
            suffix = suffix.substring(0, 16);
        }
        CRC32 crc32 = new CRC32();
        crc32.update((id.identifier() + "|" + logicalName).getBytes(StandardCharsets.UTF_8));
        String hash = String.format(Locale.ROOT, "%08x", crc32.getValue());
        String tail = "_p_" + suffix + "_" + hash;
        int parentLength = Math.min(parent.length(), 63 - tail.length());
        return parent.substring(0, parentLength) + tail;
    }

    private static String asciiIdentifier(String value) {
        String sanitized = value.replaceAll("[^A-Za-z0-9_]", "_");
        return sanitized.isEmpty() ? "table" : sanitized;
    }

    private static int decimalDigits(int value) {
        return Math.max(1, String.valueOf(value).length());
    }

    private static String listPartitionValue(String value) {
        if ("NULL".equalsIgnoreCase(value)) {
            return "NULL";
        }
        return "'" + sqlLiteral(value) + "'";
    }

    private static String rangePartitionValue(String value) {
        if ("MINVALUE".equalsIgnoreCase(value) || "MAXVALUE".equalsIgnoreCase(value)) {
            return value.toUpperCase(Locale.ROOT);
        }
        return "'" + sqlLiteral(value) + "'";
    }

    static String upsert(PostgresSinkConfig config, TableId id, Schema schema) {
        requirePrimaryKey(id, schema);
        List<String> columns = schema.getColumnNames();
        String names = columns.stream().map(PostgresSqlUtils::q).collect(Collectors.joining(","));
        String values = columns.stream().map(ignored -> "?").collect(Collectors.joining(","));
        String conflictTarget = schema.primaryKeys().stream()
                .map(PostgresSqlUtils::q)
                .collect(Collectors.joining(","));
        Set<String> primaryKeys = new HashSet<>(schema.primaryKeys());
        List<String> updates = columns.stream()
                .filter(column -> !primaryKeys.contains(column))
                .map(column -> q(column) + "=EXCLUDED." + q(column))
                .collect(Collectors.toList());
        String action = updates.isEmpty()
                ? "DO NOTHING"
                : "DO UPDATE SET " + String.join(",", updates);
        return "INSERT INTO " + tableRef(config, id) + " (" + names + ") VALUES (" + values
                + ") ON CONFLICT (" + conflictTarget + ") " + action;
    }

    static String delete(PostgresSinkConfig config, TableId id, Schema schema) {
        requirePrimaryKey(id, schema);
        return "DELETE FROM " + tableRef(config, id) + " WHERE "
                + schema.primaryKeys().stream()
                        .map(column -> q(column) + "=?")
                        .collect(Collectors.joining(" AND "));
    }

    static void requirePrimaryKey(TableId id, Schema schema) {
        if (schema.primaryKeys().isEmpty()) {
            throw new IllegalStateException(
                    "Table " + id
                            + " has no primary key; safe UPDATE/DELETE replay is impossible.");
        }
    }

    static String sqlLiteral(String value) {
        return value.replace("'", "''");
    }
}
