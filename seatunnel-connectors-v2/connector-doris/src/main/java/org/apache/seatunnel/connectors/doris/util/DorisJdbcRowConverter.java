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

package org.apache.seatunnel.connectors.doris.util;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/** Converts a JDBC result row to the schema inferred for a Doris source query. */
public final class DorisJdbcRowConverter {

    private static final ObjectMapper JSON_MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private DorisJdbcRowConverter() {}

    public static SeaTunnelRow toRow(ResultSet rows, SeaTunnelRowType rowType) throws SQLException {
        Object[] fields = new Object[rowType.getTotalFields()];
        for (int i = 0; i < fields.length; i++) {
            int column = i + 1;
            SeaTunnelDataType<?> type = rowType.getFieldType(i);
            switch (type.getSqlType()) {
                case STRING:
                    fields[i] = rows.getString(column);
                    break;
                case BOOLEAN:
                    boolean booleanValue = rows.getBoolean(column);
                    fields[i] = rows.wasNull() ? null : booleanValue;
                    break;
                case TINYINT:
                    byte byteValue = rows.getByte(column);
                    fields[i] = rows.wasNull() ? null : byteValue;
                    break;
                case SMALLINT:
                    short shortValue = rows.getShort(column);
                    fields[i] = rows.wasNull() ? null : shortValue;
                    break;
                case INT:
                    int intValue = rows.getInt(column);
                    fields[i] = rows.wasNull() ? null : intValue;
                    break;
                case BIGINT:
                    long longValue = rows.getLong(column);
                    fields[i] = rows.wasNull() ? null : longValue;
                    break;
                case FLOAT:
                    float floatValue = rows.getFloat(column);
                    fields[i] = rows.wasNull() ? null : floatValue;
                    break;
                case DOUBLE:
                    double doubleValue = rows.getDouble(column);
                    fields[i] = rows.wasNull() ? null : doubleValue;
                    break;
                case DECIMAL:
                    fields[i] = rows.getBigDecimal(column);
                    break;
                case DATE:
                    java.sql.Date date = rows.getDate(column);
                    fields[i] = date == null ? null : date.toLocalDate();
                    break;
                case TIME:
                    Time time = rows.getTime(column);
                    fields[i] = time == null ? null : time.toLocalTime();
                    break;
                case TIMESTAMP:
                    Timestamp timestamp = rows.getTimestamp(column);
                    fields[i] = timestamp == null ? null : timestamp.toLocalDateTime();
                    break;
                case BYTES:
                    fields[i] = rows.getBytes(column);
                    break;
                case ARRAY:
                case MAP:
                    String json = rows.getString(column);
                    if (json != null) {
                        try {
                            fields[i] =
                                    convertJson(
                                            JSON_MAPPER.readTree(json),
                                            type,
                                            rowType.getFieldName(i));
                        } catch (IOException | RuntimeException e) {
                            throw new SQLException(
                                    "Invalid Doris JDBC complex value for column "
                                            + rowType.getFieldName(i)
                                            + " with type "
                                            + type,
                                    e);
                        }
                    }
                    break;
                case NULL:
                    fields[i] = null;
                    break;
                default:
                    throw new SQLException(
                            "Unsupported Doris JDBC result type "
                                    + type.getSqlType()
                                    + " for column "
                                    + rowType.getFieldName(i));
            }
        }
        return new SeaTunnelRow(fields);
    }

    /** Converts nested JSON values using the declared schema, including map keys. */
    private static Object convertJson(JsonNode value, SeaTunnelDataType<?> type, String path) {
        if (value == null || value.isNull()) {
            return null;
        }
        try {
            switch (type.getSqlType()) {
                case ARRAY:
                    if (!value.isArray()) {
                        throw new IllegalArgumentException("Expected JSON array");
                    }
                    ArrayType<?, ?> arrayType = (ArrayType<?, ?>) type;
                    Object array =
                            Array.newInstance(
                                    arrayType.getElementType().getTypeClass(), value.size());
                    for (int i = 0; i < value.size(); i++) {
                        Array.set(
                                array,
                                i,
                                convertJson(
                                        value.get(i),
                                        arrayType.getElementType(),
                                        path + "[" + i + "]"));
                    }
                    return array;
                case MAP:
                    if (!value.isObject()) {
                        throw new IllegalArgumentException("Expected JSON object");
                    }
                    MapType<?, ?> mapType = (MapType<?, ?>) type;
                    Map<Object, Object> map = new LinkedHashMap<>();
                    Iterator<Map.Entry<String, JsonNode>> entries = value.fields();
                    while (entries.hasNext()) {
                        Map.Entry<String, JsonNode> entry = entries.next();
                        String entryPath = path + "[" + entry.getKey() + "]";
                        Object key =
                                convertJson(
                                        JSON_MAPPER.getNodeFactory().textNode(entry.getKey()),
                                        mapType.getKeyType(),
                                        entryPath + ".key");
                        map.put(
                                key,
                                convertJson(entry.getValue(), mapType.getValueType(), entryPath));
                    }
                    return map;
                default:
                    if (!value.isValueNode()) {
                        throw new IllegalArgumentException("Expected scalar JSON value");
                    }
                    String text = value.asText();
                    switch (type.getSqlType()) {
                        case STRING:
                            return text;
                        case BOOLEAN:
                            if (!"true".equalsIgnoreCase(text) && !"false".equalsIgnoreCase(text)) {
                                throw new IllegalArgumentException("Expected boolean");
                            }
                            return Boolean.valueOf(text);
                        case TINYINT:
                            return Byte.valueOf(text);
                        case SMALLINT:
                            return Short.valueOf(text);
                        case INT:
                            return Integer.valueOf(text);
                        case BIGINT:
                            return Long.valueOf(text);
                        case FLOAT:
                            return Float.valueOf(text);
                        case DOUBLE:
                            return Double.valueOf(text);
                        case DECIMAL:
                            return new BigDecimal(text);
                        case DATE:
                            return LocalDate.parse(text);
                        case TIME:
                            return LocalTime.parse(text);
                        case TIMESTAMP:
                            return LocalDateTime.parse(text.replace(' ', 'T'));
                        case BYTES:
                            return value.binaryValue();
                        case NULL:
                            return null;
                        default:
                            throw new IllegalArgumentException("Unsupported type " + type);
                    }
            }
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException(
                    "Cannot convert Doris JDBC value at " + path + " to " + type, e);
        }
    }

    private static Class<?> jsonValueClass(SeaTunnelDataType<?> type) {
        if (type instanceof ArrayType) {
            return Array.newInstance(jsonValueClass(((ArrayType<?, ?>) type).getElementType()), 0)
                    .getClass();
        }
        return type.getTypeClass();
    }
}
