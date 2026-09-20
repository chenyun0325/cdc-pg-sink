package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

public final class PostgresDataSink implements DataSink {
    private static final long serialVersionUID = 1L;
    private final PostgresSinkConfig config;

    public PostgresDataSink(PostgresSinkConfig config) {
        this.config = config;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkFunctionProvider.of(new PostgresEventSinkFunction(config));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new PostgresMetadataApplier(config);
    }
}
