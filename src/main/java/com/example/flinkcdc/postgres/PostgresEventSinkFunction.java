package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class PostgresEventSinkFunction extends RichSinkFunction<Event>
        implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    private final PostgresSinkConfig config;

    private transient Connection connection;
    private transient Map<TableId, Schema> schemas;
    private transient LinkedHashMap<TableId, StatementBundle> statements;
    private transient int pendingRows;
    private transient long lastFlush;
    private transient ZoneId zoneId;

    public PostgresEventSinkFunction(PostgresSinkConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.schemas = new HashMap<>();
        this.statements = new LinkedHashMap<>(16, 0.75f, true);
        this.zoneId = ZoneId.of(config.serverTimeZone);
        this.pendingRows = 0;
        this.lastFlush = System.currentTimeMillis();
        openConnection();
    }

    @Override
    public void invoke(Event event, Context context) throws Exception {
        if (event instanceof FlushEvent) {
            flushAll();
            return;
        }
        if (event instanceof CreateTableEvent) {
            CreateTableEvent create = (CreateTableEvent) event;
            invalidate(create.tableId());
            schemas.put(create.tableId(), create.getSchema());
            return;
        }
        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChange = (SchemaChangeEvent) event;
            invalidate(schemaChange.tableId());
            Schema oldSchema = schemas.get(schemaChange.tableId());
            if (event instanceof DropTableEvent) {
                schemas.remove(schemaChange.tableId());
            } else if (!(event instanceof TruncateTableEvent) && oldSchema != null) {
                schemas.put(
                        schemaChange.tableId(),
                        SchemaUtils.applySchemaChangeEvent(oldSchema, schemaChange));
            }
            return;
        }
        if (!(event instanceof DataChangeEvent)) {
            return;
        }

        DataChangeEvent dataChange = (DataChangeEvent) event;
        Schema schema = schemas.get(dataChange.tableId());
        if (schema == null) {
            throw new IllegalStateException(
                    "Missing schema for " + dataChange.tableId()
                            + "; CreateTableEvent must arrive before data events.");
        }
        PostgresSqlUtils.requirePrimaryKey(dataChange.tableId(), schema);
        StatementBundle bundle = getOrCreateBundle(dataChange.tableId(), schema);

        int addedChanges;
        switch (dataChange.op()) {
            case INSERT:
            case REPLACE:
                bundle.addUpsert(dataChange.after());
                addedChanges = 1;
                break;
            case UPDATE:
                addedChanges = bundle.addUpdate(dataChange.before(), dataChange.after());
                break;
            case DELETE:
                bundle.addDelete(dataChange.before());
                addedChanges = 1;
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported operation: " + dataChange.op());
        }
        pendingRows += addedChanges;
        long now = System.currentTimeMillis();
        if (pendingRows >= config.maxRows || now - lastFlush >= config.flushIntervalMs) {
            flushAll();
        }
    }

    private StatementBundle getOrCreateBundle(TableId id, Schema schema) throws Exception {
        StatementBundle bundle = statements.get(id);
        if (bundle != null) {
            return bundle;
        }
        if (statements.size() >= config.statementCacheSize) {
            flushAll();
            Iterator<Map.Entry<TableId, StatementBundle>> iterator =
                    statements.entrySet().iterator();
            if (iterator.hasNext()) {
                Map.Entry<TableId, StatementBundle> eldest = iterator.next();
                eldest.getValue().closeStatements();
                iterator.remove();
            }
        }
        bundle = new StatementBundle(id, schema);
        bundle.prepareStatements();
        statements.put(id, bundle);
        return bundle;
    }

    private void invalidate(TableId id) throws Exception {
        StatementBundle bundle = statements.get(id);
        if (bundle == null) {
            return;
        }
        flushAll();
        statements.remove(id);
        bundle.closeStatements();
    }

    private void flushAll() throws Exception {
        if (pendingRows == 0) {
            lastFlush = System.currentTimeMillis();
            return;
        }

        Exception failure = null;
        for (int attempt = 0; attempt <= config.maxRetries; attempt++) {
            try {
                ensureConnection();
                for (StatementBundle bundle : statements.values()) {
                    bundle.executePending();
                }
                connection.commit();
                for (StatementBundle bundle : statements.values()) {
                    bundle.clearPending();
                }
                pendingRows = 0;
                lastFlush = System.currentTimeMillis();
                return;
            } catch (Exception exception) {
                failure = exception;
                rollbackQuietly();
                clearJdbcBatchesQuietly();
                if (attempt < config.maxRetries) {
                    try {
                        reconnect();
                    } catch (Exception reconnectFailure) {
                        failure.addSuppressed(reconnectFailure);
                    }
                    Thread.sleep(Math.min(1000L * (attempt + 1), 5000L));
                }
            }
        }
        throw failure;
    }

    private void openConnection() throws Exception {
        connection = DriverManager.getConnection(
                config.jdbcUrl, config.username, config.password);
        connection.setAutoCommit(false);
    }

    private void ensureConnection() throws Exception {
        if (connection == null || connection.isClosed() || !connection.isValid(3)) {
            reconnect();
        }
    }

    private void reconnect() throws Exception {
        for (StatementBundle bundle : statements.values()) {
            bundle.closeStatements();
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ignored) {
                // The replacement connection is the recovery path.
            }
        }
        openConnection();
        for (StatementBundle bundle : statements.values()) {
            bundle.prepareStatements();
        }
    }

    private void rollbackQuietly() {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (Exception ignored) {
                // Reconnect below will discard the broken transaction.
            }
        }
    }

    private void clearJdbcBatchesQuietly() {
        for (StatementBundle bundle : statements.values()) {
            bundle.clearJdbcBatchesQuietly();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flushAll();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // JDBC writes are made replay-safe by primary-key UPSERT/DELETE semantics.
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        try {
            flushAll();
        } catch (Exception exception) {
            failure = exception;
        }
        if (statements != null) {
            for (StatementBundle bundle : statements.values()) {
                bundle.closeStatements();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception exception) {
                if (failure == null) {
                    failure = exception;
                }
            }
        }
        super.close();
        if (failure != null) {
            throw failure;
        }
    }

    private enum ChangeKind {
        UPSERT,
        DELETE
    }

    private static final class PendingChange {
        private final ChangeKind kind;
        private final Object[] values;

        private PendingChange(ChangeKind kind, Object[] values) {
            this.kind = kind;
            this.values = values;
        }
    }

    private final class StatementBundle {
        private final TableId tableId;
        private final Schema schema;
        private final List<RecordData.FieldGetter> getters;
        private final List<Integer> primaryKeyIndexes;
        private final List<PendingChange> pendingChanges;
        private transient PreparedStatement upsert;
        private transient PreparedStatement delete;

        private StatementBundle(TableId tableId, Schema schema) {
            this.tableId = tableId;
            this.schema = schema;
            this.getters = new ArrayList<>(schema.getColumnCount());
            for (int index = 0; index < schema.getColumnCount(); index++) {
                getters.add(RecordData.createFieldGetter(
                        schema.getColumnDataTypes().get(index), index));
            }
            this.primaryKeyIndexes = new ArrayList<>();
            for (String primaryKey : schema.primaryKeys()) {
                int index = schema.getColumnNames().indexOf(primaryKey);
                if (index < 0) {
                    throw new IllegalStateException(
                            "Primary key column not found: " + primaryKey);
                }
                primaryKeyIndexes.add(index);
            }
            this.pendingChanges = new ArrayList<>();
        }

        private void prepareStatements() throws Exception {
            this.upsert = connection.prepareStatement(
                    PostgresSqlUtils.upsert(config, tableId, schema));
            this.delete = connection.prepareStatement(
                    PostgresSqlUtils.delete(config, tableId, schema));
        }

        private void addUpsert(RecordData row) {
            if (row == null) {
                throw new IllegalStateException("UPSERT event has no after image");
            }
            Object[] values = new Object[getters.size()];
            for (int index = 0; index < getters.size(); index++) {
                values[index] = JdbcValueConverter.materialize(
                        getters.get(index).getFieldOrNull(row), zoneId);
            }
            pendingChanges.add(new PendingChange(ChangeKind.UPSERT, values));
        }

        private int addUpdate(RecordData before, RecordData after) {
            int addedChanges = 1;
            if (before != null && after != null && primaryKeyChanged(before, after)) {
                addDelete(before);
                addedChanges++;
            }
            addUpsert(after);
            return addedChanges;
        }

        private boolean primaryKeyChanged(RecordData before, RecordData after) {
            for (int fieldIndex : primaryKeyIndexes) {
                Object oldValue = getters.get(fieldIndex).getFieldOrNull(before);
                Object newValue = getters.get(fieldIndex).getFieldOrNull(after);
                if (!java.util.Objects.deepEquals(oldValue, newValue)) {
                    return true;
                }
            }
            return false;
        }

        private void addDelete(RecordData row) {
            if (row == null) {
                throw new IllegalStateException("DELETE event has no before image");
            }
            Object[] values = new Object[primaryKeyIndexes.size()];
            for (int index = 0; index < primaryKeyIndexes.size(); index++) {
                int fieldIndex = primaryKeyIndexes.get(index);
                values[index] = JdbcValueConverter.materialize(
                        getters.get(fieldIndex).getFieldOrNull(row), zoneId);
            }
            pendingChanges.add(new PendingChange(ChangeKind.DELETE, values));
        }

        private void executePending() throws Exception {
            ChangeKind currentKind = null;
            int batchSize = 0;
            for (PendingChange change : pendingChanges) {
                if (currentKind != null && currentKind != change.kind) {
                    executeBatch(currentKind, batchSize);
                    batchSize = 0;
                }
                PreparedStatement statement = statement(change.kind);
                for (int index = 0; index < change.values.length; index++) {
                    JdbcValueConverter.set(
                            statement, index + 1, change.values[index], zoneId);
                }
                statement.addBatch();
                currentKind = change.kind;
                batchSize++;
            }
            if (currentKind != null) {
                executeBatch(currentKind, batchSize);
            }
        }

        private PreparedStatement statement(ChangeKind kind) {
            return kind == ChangeKind.UPSERT ? upsert : delete;
        }

        private void executeBatch(ChangeKind kind, int batchSize) throws Exception {
            if (batchSize == 0) {
                return;
            }
            PreparedStatement statement = statement(kind);
            statement.executeBatch();
            statement.clearBatch();
        }

        private void clearPending() {
            pendingChanges.clear();
        }

        private void clearJdbcBatchesQuietly() {
            if (upsert != null) {
                try {
                    upsert.clearBatch();
                } catch (Exception ignored) {
                    // Best effort before a retry.
                }
            }
            if (delete != null) {
                try {
                    delete.clearBatch();
                } catch (Exception ignored) {
                    // Best effort before a retry.
                }
            }
        }

        private void closeStatements() {
            if (upsert != null) {
                try {
                    upsert.close();
                } catch (Exception ignored) {
                    // Best effort during cache eviction/reconnect.
                }
                upsert = null;
            }
            if (delete != null) {
                try {
                    delete.close();
                } catch (Exception ignored) {
                    // Best effort during cache eviction/reconnect.
                }
                delete = null;
            }
        }
    }
}
