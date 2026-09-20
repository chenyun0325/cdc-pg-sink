package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import java.util.OptionalInt;

final class PostgresTypeMapper {
    private static final int POSTGRES_MAX_LENGTH = 10_485_760;

    private PostgresTypeMapper() {}

    static String toPostgres(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                return decimal(type);
            case CHAR: {
                int length = value(DataTypes.getLength(type), 1);
                return length <= POSTGRES_MAX_LENGTH
                        ? "CHARACTER(" + Math.max(length, 1) + ")"
                        : "TEXT";
            }
            case VARCHAR: {
                int length = value(DataTypes.getLength(type), Integer.MAX_VALUE);
                return length <= POSTGRES_MAX_LENGTH
                        ? "VARCHAR(" + Math.max(length, 1) + ")"
                        : "TEXT";
            }
            case BINARY:
            case VARBINARY:
                return "BYTEA";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return "TIME(" + clamp(value(DataTypes.getPrecision(type), 0), 0, 6)
                        + ") WITHOUT TIME ZONE";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "TIMESTAMP(" + clamp(value(DataTypes.getPrecision(type), 6), 0, 6)
                        + ") WITHOUT TIME ZONE";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return "TIMESTAMP(" + clamp(value(DataTypes.getPrecision(type), 6), 0, 6)
                        + ") WITH TIME ZONE";
            case ARRAY:
            case MAP:
            case ROW:
                throw new IllegalArgumentException(
                        "Unsupported complex type for PostgreSQL sink: " + type);
            default:
                throw new IllegalArgumentException(
                        "Unsupported data type for PostgreSQL sink: " + type);
        }
    }

    private static String decimal(DataType type) {
        int precision = value(DataTypes.getPrecision(type), 38);
        int scale = value(DataTypes.getScale(type), 18);
        if (precision < 1 || precision > 1000 || scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "PostgreSQL NUMERIC requires 1 <= precision <= 1000 and 0 <= scale <= precision: "
                            + type);
        }
        return "NUMERIC(" + precision + "," + scale + ")";
    }

    private static int value(OptionalInt value, int defaultValue) {
        return value.isPresent() ? value.getAsInt() : defaultValue;
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }
}
