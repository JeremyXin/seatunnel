/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.doris.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;
import org.apache.seatunnel.connectors.doris.util.DorisJdbcRowConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;

public class DorisSqlReaderTest {

    @Test
    public void readsAggregateColumnsByPositionAndPreservesNulls() throws Exception {
        ResultSet result = Mockito.mock(ResultSet.class);
        Mockito.when(result.getObject(1)).thenReturn(10);
        Mockito.when(result.getInt(1)).thenReturn(10);
        Mockito.when(result.getObject(2)).thenReturn(2L);
        Mockito.when(result.getLong(2)).thenReturn(2L);
        Mockito.when(result.getString(3)).thenReturn(null);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"category", "total", "label"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, BasicType.LONG_TYPE, BasicType.STRING_TYPE
                        });

        SeaTunnelRow row = DorisJdbcRowConverter.toRow(result, rowType);

        Assertions.assertArrayEquals(new Object[] {10, 2L, null}, row.getFields());
    }

    @Test
    public void preservesNullNumericAndBooleanColumns() throws Exception {
        ResultSet result = Mockito.mock(ResultSet.class);
        Mockito.when(result.wasNull()).thenReturn(true);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"nullable_int", "nullable_boolean"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, BasicType.BOOLEAN_TYPE});

        SeaTunnelRow row = DorisJdbcRowConverter.toRow(result, rowType);

        Assertions.assertArrayEquals(new Object[] {null, null}, row.getFields());
    }

    @Test
    public void convertsArrayAndMapValuesToTheirDeclaredTypes() throws Exception {
        ResultSet result = Mockito.mock(ResultSet.class);
        Mockito.when(result.getString(1)).thenReturn("[1, null, 3]");
        Mockito.when(result.getString(2)).thenReturn("[\"2020-02-01\", \"2020-02-02\"]");
        Mockito.when(result.getString(3)).thenReturn("{\"1\": 10.25}");
        Mockito.when(result.getString(4)).thenReturn("{\"created\": \"2020-02-01 12:00:00\"}");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ids", "dates", "amounts", "times"},
                        new SeaTunnelDataType<?>[] {
                            ArrayType.INT_ARRAY_TYPE,
                            ArrayType.LOCAL_DATE_ARRAY_TYPE,
                            new MapType<>(BasicType.INT_TYPE, new DecimalType(10, 2)),
                            new MapType<>(BasicType.STRING_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        });

        SeaTunnelRow row = DorisJdbcRowConverter.toRow(result, rowType);

        Assertions.assertArrayEquals(new Integer[] {1, null, 3}, (Object[]) row.getField(0));
        Assertions.assertArrayEquals(
                new LocalDate[] {LocalDate.of(2020, 2, 1), LocalDate.of(2020, 2, 2)},
                (Object[]) row.getField(1));
        Assertions.assertEquals(Map.of(1, new java.math.BigDecimal("10.25")), row.getField(2));
        Assertions.assertEquals(
                Map.of("created", LocalDateTime.of(2020, 2, 1, 12, 0)), row.getField(3));
    }

    @Test
    public void preservesNullAndEmptyComplexValues() throws Exception {
        ResultSet result = Mockito.mock(ResultSet.class);
        Mockito.when(result.getString(1)).thenReturn(null);
        Mockito.when(result.getString(2)).thenReturn("[]");
        Mockito.when(result.getString(3)).thenReturn(null);
        Mockito.when(result.getString(4)).thenReturn("{}");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"missing_array", "empty_array", "missing_map", "empty_map"},
                        new SeaTunnelDataType<?>[] {
                            ArrayType.INT_ARRAY_TYPE,
                            ArrayType.INT_ARRAY_TYPE,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.BOOLEAN_TYPE),
                            new MapType<>(BasicType.STRING_TYPE, BasicType.BOOLEAN_TYPE)
                        });

        SeaTunnelRow row = DorisJdbcRowConverter.toRow(result, rowType);

        Assertions.assertNull(row.getField(0));
        Assertions.assertArrayEquals(new Integer[0], (Object[]) row.getField(1));
        Assertions.assertNull(row.getField(2));
        Assertions.assertEquals(Map.of(), row.getField(3));
    }

    @Test
    public void convertsNestedArraysAndMaps() throws Exception {
        ResultSet result = Mockito.mock(ResultSet.class);
        Mockito.when(result.getString(1))
                .thenReturn("[{\"2020-02-01\": [true, null, false]}, null, {}]");
        Mockito.when(result.getString(2)).thenReturn("{\"amount\": 1234567890.123456789}");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"nested", "precise"},
                        new SeaTunnelDataType<?>[] {
                            ArrayType.of(
                                    new MapType<>(
                                            LocalTimeType.LOCAL_DATE_TYPE,
                                            ArrayType.BOOLEAN_ARRAY_TYPE)),
                            new MapType<>(BasicType.STRING_TYPE, new DecimalType(25, 9))
                        });

        SeaTunnelRow row = DorisJdbcRowConverter.toRow(result, rowType);

        Object[] nested = (Object[]) row.getField(0);
        Assertions.assertArrayEquals(
                new Boolean[] {true, null, false},
                (Object[]) ((Map<?, ?>) nested[0]).get(LocalDate.of(2020, 2, 1)));
        Assertions.assertNull(nested[1]);
        Assertions.assertEquals(Map.of(), nested[2]);
        Assertions.assertEquals(
                Map.of("amount", new java.math.BigDecimal("1234567890.123456789")),
                row.getField(1));
    }

    @Test
    public void invalidComplexValuesIncludeColumnAndNestedPath() throws Exception {
        ResultSet result = Mockito.mock(ResultSet.class);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ids"},
                        new SeaTunnelDataType<?>[] {ArrayType.INT_ARRAY_TYPE});
        for (String json : new String[] {"{}", "[1, \"invalid\"]", "["}) {
            Mockito.when(result.getString(1)).thenReturn(json);
            SQLException error =
                    Assertions.assertThrows(
                            SQLException.class, () -> DorisJdbcRowConverter.toRow(result, rowType));
            Assertions.assertTrue(error.getMessage().contains("ids"));
            Assertions.assertNotNull(error.getCause());
        }
        Mockito.when(result.getString(1)).thenReturn("[1, \"invalid\"]");
        SQLException error =
                Assertions.assertThrows(
                        SQLException.class, () -> DorisJdbcRowConverter.toRow(result, rowType));
        Assertions.assertTrue(error.getCause().getCause().getMessage().contains("ids[1]"));
    }

    @Test
    public void rejectsUnsupportedJdbcResultTypeWithColumnName() {
        ResultSet result = Mockito.mock(ResultSet.class);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"items"},
                        new SeaTunnelDataType<?>[] {
                            new SeaTunnelRowType(
                                    new String[] {"nested"},
                                    new SeaTunnelDataType<?>[] {BasicType.INT_TYPE})
                        });

        SQLException error =
                Assertions.assertThrows(
                        SQLException.class, () -> DorisJdbcRowConverter.toRow(result, rowType));

        Assertions.assertTrue(error.getMessage().contains("items"));
        Assertions.assertTrue(error.getMessage().contains("ROW"));
    }

    @Test
    public void cancelledReaderDoesNotOpenConnection() {
        DorisSqlReader reader =
                new DorisSqlReader(
                        Mockito.mock(DorisSourceConfig.class),
                        Mockito.mock(DorisSourceTable.class));
        reader.cancel();

        Assertions.assertThrows(
                IllegalStateException.class, () -> reader.read(Mockito.mock(Collector.class)));
    }

    @Test
    public void cancellingActiveReadAbortsStatementAndConnection() throws Exception {
        DorisSourceConfig config = Mockito.mock(DorisSourceConfig.class);
        Mockito.when(config.getFrontends()).thenReturn("fe:8030");
        Mockito.when(config.getQueryPort()).thenReturn(9030);
        Mockito.when(config.getUsername()).thenReturn("root");
        Mockito.when(config.getPassword()).thenReturn("");
        Mockito.when(config.getRequestConnectTimeoutMs()).thenReturn(1000);
        Mockito.when(config.getRequestReadTimeoutMs()).thenReturn(1000);
        Mockito.when(config.getRequestQueryTimeoutS()).thenReturn(10);
        DorisSourceTable table = Mockito.mock(DorisSourceTable.class);
        Mockito.when(table.getTablePath()).thenReturn(TablePath.of("source_db", "source_table"));
        Mockito.when(table.getSql()).thenReturn("SELECT 1");
        CatalogTable catalogTable = Mockito.mock(CatalogTable.class);
        Mockito.when(table.getCatalogTable()).thenReturn(catalogTable);
        Mockito.when(catalogTable.getSeaTunnelRowType())
                .thenReturn(
                        new SeaTunnelRowType(
                                new String[] {"value"},
                                new SeaTunnelDataType<?>[] {BasicType.INT_TYPE}));
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet result = Mockito.mock(ResultSet.class);
        Mockito.when(
                        connection.prepareStatement(
                                "SELECT 1",
                                ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(result);
        DorisSqlReader reader = new DorisSqlReader(config, table);
        Mockito.when(result.next())
                .thenAnswer(
                        invocation -> {
                            reader.cancel();
                            throw new SQLException("cancelled");
                        });

        try (MockedStatic<DriverManager> driver = Mockito.mockStatic(DriverManager.class)) {
            driver.when(
                            () ->
                                    DriverManager.getConnection(
                                            Mockito.eq("jdbc:mysql://fe:9030/source_db"),
                                            Mockito.any(Properties.class)))
                    .thenReturn(connection);
            Assertions.assertThrows(
                    RuntimeException.class, () -> reader.read(Mockito.mock(Collector.class)));
        }

        Mockito.verify(statement, Mockito.atLeastOnce()).cancel();
        Mockito.verify(connection, Mockito.atLeastOnce()).close();
    }
}
