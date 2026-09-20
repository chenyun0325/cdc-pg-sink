package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public final class PostgresMetadataApplier implements MetadataApplier {
    private static final long serialVersionUID = 1L;

    private final PostgresSinkConfig config;
    private transient Connection connection;

    public PostgresMetadataApplier(PostgresSinkConfig config) {
        this.config = config;
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return EnumSet.allOf(SchemaChangeEventType.class);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        try {
            ensureConnection();
            TableId id = event.tableId();
            if (event instanceof CreateTableEvent) {
                applyCreateTable(id, ((CreateTableEvent) event).getSchema());
            } else if (event instanceof AddColumnEvent) {
                applyAddColumns(id, (AddColumnEvent) event);
            } else if (event instanceof RenameColumnEvent) {
                applyRenameColumns(id, (RenameColumnEvent) event);
            } else if (event instanceof AlterColumnTypeEvent) {
                applyAlterColumnTypes(id, (AlterColumnTypeEvent) event);
            } else if (event instanceof DropColumnEvent) {
                rejectDangerous(event);
                for (String column : ((DropColumnEvent) event).getDroppedColumnNames()) {
                    execute("ALTER TABLE " + PostgresSqlUtils.tableRef(config, id)
                            + " DROP COLUMN " + PostgresSqlUtils.q(column));
                }
            } else if (event instanceof DropTableEvent) {
                rejectDangerous(event);
                execute("DROP TABLE IF EXISTS " + PostgresSqlUtils.tableRef(config, id));
            } else if (event instanceof TruncateTableEvent) {
                rejectDangerous(event);
                execute("TRUNCATE TABLE " + PostgresSqlUtils.tableRef(config, id));
            } else {
                throw new UnsupportedOperationException("Unsupported schema change: " + event);
            }
        } catch (SQLException exception) {
            throw new RuntimeException(
                    "Failed to apply PostgreSQL schema change: " + event, exception);
        }
    }

    private void applyCreateTable(TableId id, Schema schema) throws SQLException {
        if (config.createSchema) {
            execute("CREATE SCHEMA IF NOT EXISTS "
                    + PostgresSqlUtils.q(PostgresSqlUtils.schema(config, id)));
        }
        execute(PostgresSqlUtils.createTable(config, id, schema));
        for (String partitionSql : PostgresSqlUtils.createPartitions(config, id, schema)) {
            execute(partitionSql);
        }
        if (schema.comment() != null && !schema.comment().isBlank()) {
            execute("COMMENT ON TABLE " + PostgresSqlUtils.tableRef(config, id) + " IS '"
                    + PostgresSqlUtils.sqlLiteral(schema.comment()) + "'");
        }
        for (Column column : schema.getColumns()) {
            applyColumnComment(id, column);
        }
    }

    private void applyAddColumns(TableId id, AddColumnEvent event) throws SQLException {
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            Column column = columnWithPosition.getAddColumn();
            StringBuilder sql = new StringBuilder("ALTER TABLE ")
                    .append(PostgresSqlUtils.tableRef(config, id))
                    .append(" ADD COLUMN ")
                    .append(PostgresSqlUtils.q(column.getName())).append(' ')
                    .append(PostgresTypeMapper.toPostgres(column.getType()));
            if (!column.getType().isNullable()) {
                sql.append(" NOT NULL");
            }
            execute(sql.toString());
            applyColumnComment(id, column);
        }
    }

    private void applyRenameColumns(TableId id, RenameColumnEvent event) throws SQLException {
        for (Map.Entry<String, String> rename : event.getNameMapping().entrySet()) {
            execute("ALTER TABLE " + PostgresSqlUtils.tableRef(config, id)
                    + " RENAME COLUMN " + PostgresSqlUtils.q(rename.getKey())
                    + " TO " + PostgresSqlUtils.q(rename.getValue()));
        }
    }

    private void applyAlterColumnTypes(TableId id, AlterColumnTypeEvent event)
            throws SQLException {
        for (Map.Entry<String, DataType> change : event.getTypeMapping().entrySet()) {
            String column = PostgresSqlUtils.q(change.getKey());
            String type = PostgresTypeMapper.toPostgres(change.getValue());
            execute("ALTER TABLE " + PostgresSqlUtils.tableRef(config, id)
                    + " ALTER COLUMN " + column + " TYPE " + type
                    + " USING " + column + "::" + type);
            execute("ALTER TABLE " + PostgresSqlUtils.tableRef(config, id)
                    + " ALTER COLUMN " + column
                    + (change.getValue().isNullable() ? " DROP NOT NULL" : " SET NOT NULL"));
        }
    }

    private void applyColumnComment(TableId id, Column column) throws SQLException {
        if (column.getComment() == null || column.getComment().isBlank()) {
            return;
        }
        execute("COMMENT ON COLUMN " + PostgresSqlUtils.tableRef(config, id) + "."
                + PostgresSqlUtils.q(column.getName()) + " IS '"
                + PostgresSqlUtils.sqlLiteral(column.getComment()) + "'");
    }

    private void rejectDangerous(SchemaChangeEvent event) {
        if (!config.dangerousDdlEnabled) {
            throw new IllegalStateException(
                    "Destructive DDL is disabled: " + event
                            + ". Set dangerous-ddl.enabled=true only after risk review.");
        }
    }

    private void ensureConnection() throws SQLException {
        if (connection == null || connection.isClosed() || !connection.isValid(3)) {
            closeQuietly();
            connection = DriverManager.getConnection(
                    config.jdbcUrl, config.username, config.password);
            connection.setAutoCommit(true);
        }
    }

    private void execute(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ignored) {
                // Best effort during shutdown/reconnect.
            }
        }
        connection = null;
    }
}
