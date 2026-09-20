package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.sink.DataSink;

import java.util.Collections;
import java.util.Set;

public final class PostgresDataSinkFactory implements DataSinkFactory {
    @Override
    public DataSink createDataSink(Context context) {
        PostgresSinkConfig config =
                PostgresSinkConfig.from(context.getFactoryConfiguration().toMap());
        return new PostgresDataSink(config);
    }

    @Override
    public String identifier() {
        return "postgres";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
