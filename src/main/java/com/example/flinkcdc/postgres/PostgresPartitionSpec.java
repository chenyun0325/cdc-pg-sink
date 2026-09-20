package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

final class PostgresPartitionSpec {
    static final String STRATEGY_OPTION = "postgres.partition.strategy";
    static final String COUNT_OPTION = "postgres.partition.count";
    static final String DEFAULT_OPTION = "postgres.partition.default";
    static final String LIST_PREFIX = "postgres.partition.list.";
    static final String RANGE_PREFIX = "postgres.partition.range.";

    enum Strategy {
        HASH,
        LIST,
        RANGE
    }

    static final class NamedPartition {
        final String name;
        final List<String> values;

        NamedPartition(String name, List<String> values) {
            this.name = name;
            this.values = Collections.unmodifiableList(new ArrayList<>(values));
        }
    }

    final Strategy strategy;
    final List<String> columns;
    final int hashPartitionCount;
    final List<NamedPartition> partitions;
    final boolean createDefaultPartition;

    private PostgresPartitionSpec(
            Strategy strategy,
            List<String> columns,
            int hashPartitionCount,
            List<NamedPartition> partitions,
            boolean createDefaultPartition) {
        this.strategy = strategy;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.hashPartitionCount = hashPartitionCount;
        this.partitions = Collections.unmodifiableList(new ArrayList<>(partitions));
        this.createDefaultPartition = createDefaultPartition;
    }

    static Optional<PostgresPartitionSpec> from(
            PostgresSinkConfig config, TableId tableId, Schema schema) {
        if (schema.partitionKeys().isEmpty()) {
            return Optional.empty();
        }

        List<String> columns = schema.partitionKeys();
        validateColumns(tableId, schema, columns);

        Map<String, String> options = schema.options();
        Strategy strategy = parseStrategy(
                options.getOrDefault(STRATEGY_OPTION, config.partitionDefaultStrategy),
                tableId);
        int count = parseCount(
                options.getOrDefault(COUNT_OPTION, String.valueOf(config.partitionDefaultCount)),
                tableId);
        boolean defaultPartition = Boolean.parseBoolean(
                options.getOrDefault(DEFAULT_OPTION, "false").trim());

        if (strategy == Strategy.HASH && defaultPartition) {
            throw new IllegalArgumentException(
                    "PostgreSQL HASH partitioning does not support a default partition: "
                            + tableId);
        }

        if ((strategy == Strategy.LIST || strategy == Strategy.RANGE)
                && columns.size() != 1) {
            throw new IllegalArgumentException(
                    "PostgreSQL " + strategy.name().toLowerCase(Locale.ROOT)
                            + " partitioning requires exactly one partition key in this sink: "
                            + tableId);
        }

        List<NamedPartition> definitions;
        if (strategy == Strategy.LIST) {
            definitions = parseDefinitions(options, LIST_PREFIX, tableId, false);
        } else if (strategy == Strategy.RANGE) {
            definitions = parseDefinitions(options, RANGE_PREFIX, tableId, true);
        } else {
            definitions = Collections.emptyList();
        }

        if ((strategy == Strategy.LIST || strategy == Strategy.RANGE)
                && definitions.isEmpty() && !defaultPartition) {
            throw new IllegalArgumentException(
                    "Partitioned table " + tableId + " has no "
                            + strategy.name().toLowerCase(Locale.ROOT)
                            + " partition definitions and no default partition.");
        }

        return Optional.of(new PostgresPartitionSpec(
                strategy, columns, count, definitions, defaultPartition));
    }

    private static void validateColumns(TableId tableId, Schema schema, List<String> columns) {
        for (String column : columns) {
            if (!schema.getColumnNames().contains(column)) {
                throw new IllegalArgumentException(
                        "Partition key " + column + " does not exist in " + tableId);
            }
            if (!schema.primaryKeys().contains(column)) {
                throw new IllegalArgumentException(
                        "PostgreSQL primary key/unique constraint on partitioned table "
                                + tableId + " must include partition key " + column
                                + ". Add it to transform.primary-keys.");
            }
        }
    }

    private static Strategy parseStrategy(String value, TableId tableId) {
        try {
            return Strategy.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException(
                    "Unknown PostgreSQL partition strategy for " + tableId + ": " + value,
                    exception);
        }
    }

    private static int parseCount(String value, TableId tableId) {
        try {
            int count = Integer.parseInt(value.trim());
            if (count <= 0 || count > 1024) {
                throw new IllegalArgumentException(
                        "PostgreSQL hash partition count must be between 1 and 1024 for "
                                + tableId);
            }
            return count;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Invalid PostgreSQL hash partition count for " + tableId + ": " + value,
                    exception);
        }
    }

    private static List<NamedPartition> parseDefinitions(
            Map<String, String> options,
            String prefix,
            TableId tableId,
            boolean range) {
        Map<String, String> sorted = new TreeMap<>();
        for (Map.Entry<String, String> option : options.entrySet()) {
            if (option.getKey().startsWith(prefix)) {
                String name = option.getKey().substring(prefix.length()).trim();
                if (name.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Empty PostgreSQL partition name in options for " + tableId);
                }
                sorted.put(name, option.getValue());
            }
        }

        Map<String, NamedPartition> parsed = new LinkedHashMap<>();
        for (Map.Entry<String, String> definition : sorted.entrySet()) {
            String[] tokens = definition.getValue().split("\\|", -1);
            List<String> values = new ArrayList<>(tokens.length);
            for (String token : tokens) {
                String value = token.trim();
                if (value.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Empty PostgreSQL partition bound in " + definition.getKey()
                                    + " for " + tableId);
                }
                values.add(value);
            }
            if (range && values.size() != 2) {
                throw new IllegalArgumentException(
                        "Range partition " + definition.getKey()
                                + " must have exactly FROM|TO bounds for " + tableId);
            }
            parsed.put(
                    definition.getKey(),
                    new NamedPartition(definition.getKey(), values));
        }
        return new ArrayList<>(parsed.values());
    }
}
