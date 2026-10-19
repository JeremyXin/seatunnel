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

package org.apache.seatunnel.connectors.doris.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.connectors.doris.datatype.DorisTypeConverterV2;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

public class DorisCatalogSqlTest {

    @Test
    public void preservesComplexTypesOnlyForUnaliasedColumnsFromConfiguredTable() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement metadataStatement = Mockito.mock(PreparedStatement.class);
        PreparedStatement schemaStatement = Mockito.mock(PreparedStatement.class);
        ResultSet metadataResult = Mockito.mock(ResultSet.class);
        ResultSet schemaResult = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);

        Mockito.when(
                        connection.prepareStatement(
                                "SELECT * FROM (SELECT array_col, map_col, array_col AS renamed FROM source_table\n) AS doris_source_query LIMIT 0"))
                .thenReturn(metadataStatement);
        Mockito.when(
                        connection.prepareStatement(
                                org.apache.seatunnel.connectors.doris.util.DorisCatalogUtil
                                        .TABLE_SCHEMA_QUERY))
                .thenReturn(schemaStatement);
        Mockito.when(metadataStatement.executeQuery()).thenReturn(metadataResult);
        Mockito.when(schemaStatement.executeQuery()).thenReturn(schemaResult);
        Mockito.when(metadataResult.getMetaData()).thenReturn(metadata);
        Mockito.when(schemaResult.next()).thenReturn(true, true, false);
        Mockito.when(schemaResult.getString("COLUMN_NAME")).thenReturn("array_col", "map_col");
        Mockito.when(schemaResult.getString("COLUMN_TYPE"))
                .thenReturn("ARRAY<INT(11)>", "MAP<INT,VARCHAR(20)>");
        Mockito.when(schemaResult.getString("DATA_TYPE")).thenReturn("ARRAY", "MAP");
        Mockito.when(schemaResult.getString("IS_NULLABLE")).thenReturn("YES", "YES");

        Mockito.when(metadata.getColumnCount()).thenReturn(3);
        Mockito.when(metadata.getColumnType(Mockito.anyInt())).thenReturn(Types.VARCHAR);
        Mockito.when(metadata.getColumnTypeName(Mockito.anyInt())).thenReturn("VARCHAR");
        Mockito.when(metadata.getTableName(Mockito.anyInt())).thenReturn("source_table");
        Mockito.when(metadata.getColumnName(1)).thenReturn("array_col");
        Mockito.when(metadata.getColumnLabel(1)).thenReturn("array_col");
        Mockito.when(metadata.getColumnName(2)).thenReturn("map_col");
        Mockito.when(metadata.getColumnLabel(2)).thenReturn("map_col");
        Mockito.when(metadata.getColumnName(3)).thenReturn("array_col");
        Mockito.when(metadata.getColumnLabel(3)).thenReturn("renamed");

        DorisCatalog catalog = new DorisCatalog("doris", "localhost:8030", 9030, "root", "");
        ReflectionUtils.setField(catalog, DorisCatalog.class, "conn", connection);
        ReflectionUtils.setField(
                catalog, DorisCatalog.class, "typeConverter", DorisTypeConverterV2.INSTANCE);

        CatalogTable table =
                catalog.getTableBySql(
                        TablePath.of("source_db", "source_table"),
                        "SELECT array_col, map_col, array_col AS renamed FROM source_table");

        Assertions.assertEquals(
                org.apache.seatunnel.api.table.type.ArrayType.INT_ARRAY_TYPE,
                table.getSeaTunnelRowType().getFieldType(0));
        Assertions.assertEquals(
                new MapType<>(BasicType.INT_TYPE, BasicType.STRING_TYPE),
                table.getSeaTunnelRowType().getFieldType(1));
        Assertions.assertEquals(BasicType.STRING_TYPE, table.getSeaTunnelRowType().getFieldType(2));
        Mockito.verify(schemaStatement).setString(1, "source_db");
        Mockito.verify(schemaStatement).setString(2, "source_table");
    }

    @Test
    public void queryMetadataDefinesAliasesAndAggregateTypes() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet result = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
        Mockito.when(connection.getCatalog()).thenReturn("information_schema");
        Mockito.when(statement.executeQuery()).thenReturn(result);
        Mockito.when(result.getMetaData()).thenReturn(metadata);
        Mockito.when(metadata.getColumnCount()).thenReturn(3);
        Mockito.when(metadata.getColumnLabel(1)).thenReturn("category");
        Mockito.when(metadata.getColumnTypeName(1)).thenReturn("INT");
        Mockito.when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
        Mockito.when(metadata.getPrecision(1)).thenReturn(11);
        Mockito.when(metadata.getColumnLabel(2)).thenReturn("total");
        Mockito.when(metadata.getColumnTypeName(2)).thenReturn("BIGINT");
        Mockito.when(metadata.getColumnType(2)).thenReturn(Types.BIGINT);
        Mockito.when(metadata.getPrecision(2)).thenReturn(19);
        Mockito.when(metadata.getColumnLabel(3)).thenReturn("amount");
        Mockito.when(metadata.getColumnTypeName(3)).thenReturn("DECIMAL");
        Mockito.when(metadata.getColumnType(3)).thenReturn(Types.DECIMAL);
        Mockito.when(metadata.getPrecision(3)).thenReturn(12);
        Mockito.when(metadata.getScale(3)).thenReturn(2);

        DorisCatalog catalog = new DorisCatalog("doris", "localhost:8030", 9030, "root", "");
        ReflectionUtils.setField(catalog, DorisCatalog.class, "conn", connection);
        ReflectionUtils.setField(
                catalog, DorisCatalog.class, "typeConverter", DorisTypeConverterV2.INSTANCE);

        CatalogTable table =
                catalog.getTableBySql(
                        TablePath.of("source_db", "source_table"),
                        "SELECT v AS category, COUNT(*) AS total FROM source_table GROUP BY v");

        Assertions.assertArrayEquals(
                new String[] {"category", "total", "amount"},
                table.getSeaTunnelRowType().getFieldNames());
        Assertions.assertEquals(BasicType.INT_TYPE, table.getSeaTunnelRowType().getFieldType(0));
        Assertions.assertEquals(BasicType.LONG_TYPE, table.getSeaTunnelRowType().getFieldType(1));
        Assertions.assertEquals(
                new DecimalType(12, 2), table.getSeaTunnelRowType().getFieldType(2));
        Mockito.verify(connection).setCatalog("source_db");
        Mockito.verify(connection).setCatalog("information_schema");
        Mockito.verify(connection)
                .prepareStatement(
                        "SELECT * FROM (SELECT v AS category, COUNT(*) AS total FROM source_table GROUP BY v\n) AS doris_source_query LIMIT 0");

        catalog.getTableBySql(
                TablePath.of("source_db", "source_table"),
                "SELECT v AS category, COUNT(*) AS total FROM source_table GROUP BY v -- trailing comment");
        Mockito.verify(connection)
                .prepareStatement(
                        "SELECT * FROM (SELECT v AS category, COUNT(*) AS total FROM source_table GROUP BY v -- trailing comment\n) AS doris_source_query LIMIT 0");
    }
}
