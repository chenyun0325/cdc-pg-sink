package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.event.TableId;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

public final class PostgresSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String hostname;
    public final int port;
    public final String databaseName;
    public final String username;
    public final String password;
    public final String jdbcUrl;
    public final String schemaName;
    public final String schemaPrefix;
    public final String defaultSchema;
    public final boolean createSchema;
    public final int maxRows;
    public final long flushIntervalMs;
    public final int maxRetries;
    public final int statementCacheSize;
    public final boolean dangerousDdlEnabled;
    public final String noPrimaryKeyAction;
    public final String serverTimeZone;
    public final String partitionDefaultStrategy;
    public final int partitionDefaultCount;

    private PostgresSinkConfig(Map<String, String> options) {
        String configuredUrl = trimToNull(options.get("jdbc-url"));
        this.hostname = configuredUrl == null ? required(options, "hostname") : options.getOrDefault("hostname", "");
        this.port = intValue(options, "port", 5432);
        this.databaseName = required(options, "database-name");
        this.username = required(options, "username");
        this.password = options.getOrDefault("password", "");
        this.schemaName = options.getOrDefault("schema-name", "").trim();
        this.schemaPrefix = options.getOrDefault("schema-prefix", "");
        this.defaultSchema = nonBlank(options.getOrDefault("default-schema", "public"), "default-schema");
        this.createSchema = boolValue(options, "create-schema.enabled", true);
        this.maxRows = intValue(options, "sink.buffer-flush.max-rows", 1000);
        this.flushIntervalMs = durationMs(options.getOrDefault("sink.buffer-flush.interval", "2s"));
        this.maxRetries = intValue(options, "sink.max-retries", 3);
        this.statementCacheSize = intValue(options, "statement-cache.max-size", 256);
        this.dangerousDdlEnabled = boolValue(options, "dangerous-ddl.enabled", false);
        this.noPrimaryKeyAction = options.getOrDefault("no-primary-key.action", "fail").toLowerCase(Locale.ROOT);
        this.serverTimeZone = options.getOrDefault("server-time-zone", "Asia/Shanghai");
        this.partitionDefaultStrategy = options
                .getOrDefault("partition.default-strategy", "hash")
                .trim()
                .toLowerCase(Locale.ROOT);
        this.partitionDefaultCount = intValue(options, "partition.default-count", 16);
        this.jdbcUrl = configuredUrl == null ? defaultJdbcUrl() : configuredUrl;

        if (!"fail".equals(noPrimaryKeyAction)) {
            throw new IllegalArgumentException(
                    "Only no-primary-key.action=fail is supported for safe CDC semantics.");
        }
        if (port <= 0 || port > 65535 || maxRows <= 0 || flushIntervalMs <= 0
                || statementCacheSize <= 0 || maxRetries < 0
                || partitionDefaultCount <= 0 || partitionDefaultCount > 1024) {
            throw new IllegalArgumentException("Invalid PostgreSQL sink numeric options.");
        }
        if (!"hash".equals(partitionDefaultStrategy)
                && !"list".equals(partitionDefaultStrategy)
                && !"range".equals(partitionDefaultStrategy)) {
            throw new IllegalArgumentException(
                    "partition.default-strategy must be hash, list, or range.");
        }
    }

    public static PostgresSinkConfig from(Map<String, String> options) {
        return new PostgresSinkConfig(options);
    }

    public String targetSchema(TableId id) {
        if (!schemaName.isEmpty()) {
            return schemaName;
        }
        String sourceSchema = trimToNull(id.getSchemaName());
        if (sourceSchema == null) {
            sourceSchema = trimToNull(id.getNamespace());
        }
        if (sourceSchema == null) {
            sourceSchema = defaultSchema;
        }
        return schemaPrefix + sourceSchema;
    }

    private String defaultJdbcUrl() {
        return "jdbc:postgresql://" + hostname + ":" + port + "/" + urlEncode(databaseName)
                + "?reWriteBatchedInserts=true&ApplicationName=flink-cdc-postgres-sink";
    }

    private static String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String required(Map<String, String> options, String key) {
        return nonBlank(options.get(key), key);
    }

    private static String nonBlank(String value, String key) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required sink option: " + key);
        }
        return value.trim();
    }

    private static String trimToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private static int intValue(Map<String, String> options, String key, int defaultValue) {
        String value = options.get(key);
        return value == null ? defaultValue : Integer.parseInt(value.trim());
    }

    private static boolean boolValue(Map<String, String> options, String key, boolean defaultValue) {
        String value = options.get(key);
        return value == null ? defaultValue : Boolean.parseBoolean(value.trim());
    }

    static long durationMs(String text) {
        String value = text.trim().toLowerCase(Locale.ROOT);
        if (value.endsWith("ms")) {
            return Long.parseLong(value.substring(0, value.length() - 2));
        }
        if (value.endsWith("s")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 1000L;
        }
        if (value.endsWith("m")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 60_000L;
        }
        if (value.endsWith("h")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 3_600_000L;
        }
        return Long.parseLong(value);
    }
}
