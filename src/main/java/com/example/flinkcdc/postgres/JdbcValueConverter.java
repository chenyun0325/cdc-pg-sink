package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.math.BigDecimal;
import java.time.ZoneId;

final class JdbcValueConverter {
    private JdbcValueConverter() {}

    static Object materialize(Object value, ZoneId zone) {
        if (value instanceof StringData) {
            return value.toString();
        }
        if (value instanceof DecimalData) {
            return ((DecimalData) value).toBigDecimal();
        }
        if (value instanceof DateData) {
            return ((DateData) value).toLocalDate();
        }
        if (value instanceof TimeData) {
            return ((TimeData) value).toLocalTime();
        }
        if (value instanceof TimestampData) {
            return ((TimestampData) value).toLocalDateTime();
        }
        if (value instanceof LocalZonedTimestampData) {
            return ((LocalZonedTimestampData) value)
                    .toInstant()
                    .atZone(zone)
                    .toOffsetDateTime();
        }
        if (value instanceof ZonedTimestampData) {
            return ((ZonedTimestampData) value).getZonedDateTime().toOffsetDateTime();
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        return value;
    }

    static void set(PreparedStatement statement, int index, Object value, ZoneId zone)
            throws SQLException {
        Object jdbcValue = materialize(value, zone);
        if (jdbcValue == null) {
            statement.setObject(index, null);
        } else if (jdbcValue instanceof String) {
            statement.setString(index, (String) jdbcValue);
        } else if (jdbcValue instanceof BigDecimal) {
            statement.setBigDecimal(index, (BigDecimal) jdbcValue);
        } else if (jdbcValue instanceof byte[]) {
            statement.setBytes(index, (byte[]) jdbcValue);
        } else {
            statement.setObject(index, jdbcValue);
        }
    }
}
