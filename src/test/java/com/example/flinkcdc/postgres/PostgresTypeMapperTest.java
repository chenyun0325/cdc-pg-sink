package com.example.flinkcdc.postgres;

import org.apache.flink.cdc.common.types.DataTypes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostgresTypeMapperTest {
    @Test
    void mapsScalarTypes() {
        assertEquals("BOOLEAN", PostgresTypeMapper.toPostgres(DataTypes.BOOLEAN()));
        assertEquals("SMALLINT", PostgresTypeMapper.toPostgres(DataTypes.TINYINT()));
        assertEquals("INTEGER", PostgresTypeMapper.toPostgres(DataTypes.INT()));
        assertEquals("BIGINT", PostgresTypeMapper.toPostgres(DataTypes.BIGINT()));
        assertEquals("REAL", PostgresTypeMapper.toPostgres(DataTypes.FLOAT()));
        assertEquals("DOUBLE PRECISION", PostgresTypeMapper.toPostgres(DataTypes.DOUBLE()));
        assertEquals("NUMERIC(20,4)", PostgresTypeMapper.toPostgres(DataTypes.DECIMAL(20, 4)));
        assertEquals("VARCHAR(255)", PostgresTypeMapper.toPostgres(DataTypes.VARCHAR(255)));
        assertEquals("TEXT", PostgresTypeMapper.toPostgres(DataTypes.STRING()));
        assertEquals("BYTEA", PostgresTypeMapper.toPostgres(DataTypes.VARBINARY(1024)));
        assertEquals("DATE", PostgresTypeMapper.toPostgres(DataTypes.DATE()));
        assertEquals(
                "TIME(3) WITHOUT TIME ZONE",
                PostgresTypeMapper.toPostgres(DataTypes.TIME(3)));
        assertEquals(
                "TIMESTAMP(6) WITHOUT TIME ZONE",
                PostgresTypeMapper.toPostgres(DataTypes.TIMESTAMP(6)));
        assertEquals(
                "TIMESTAMP(6) WITH TIME ZONE",
                PostgresTypeMapper.toPostgres(DataTypes.TIMESTAMP_LTZ(6)));
        assertEquals(
                "TIMESTAMP(6) WITH TIME ZONE",
                PostgresTypeMapper.toPostgres(DataTypes.TIMESTAMP_TZ(6)));
    }

    @Test
    void rejectsComplexTypes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> PostgresTypeMapper.toPostgres(DataTypes.ARRAY(DataTypes.INT())));
    }
}
