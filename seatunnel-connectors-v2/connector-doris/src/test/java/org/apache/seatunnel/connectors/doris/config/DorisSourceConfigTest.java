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

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DorisSourceConfigTest {

    @Test
    public void customSqlIsRetainedForSingleTable() {
        Map<String, Object> options = baseOptions();
        options.put("sql", "SELECT F_ID FROM source_table GROUP BY F_ID");

        DorisTableConfig table = DorisTableConfig.of(ReadonlyConfig.fromMap(options)).get(0);

        Assertions.assertEquals("SELECT F_ID FROM source_table GROUP BY F_ID", table.getSql());
    }

    @Test
    public void customSqlIsRetainedInTableList() {
        Map<String, Object> options = baseOptions();
        Map<String, Object> table = new HashMap<>();
        table.put("database", "source_db");
        table.put("table", "source_table");
        table.put("sql", "SELECT F_ID FROM source_table");
        options.put("table_list", Collections.singletonList(table));

        Assertions.assertEquals(
                "SELECT F_ID FROM source_table",
                DorisTableConfig.of(ReadonlyConfig.fromMap(options)).get(0).getSql());
    }

    @Test
    public void customSqlRejectsFilterQuery() {
        Map<String, Object> options = baseOptions();
        options.put("sql", "SELECT F_ID FROM source_table");
        options.put("doris.filter.query", "F_ID > 1");

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> DorisTableConfig.of(ReadonlyConfig.fromMap(options)));
        Assertions.assertTrue(error.getMessage().contains("doris.filter.query"));
    }

    @Test
    public void customSqlRejectsReadField() {
        Map<String, Object> options = baseOptions();
        options.put("sql", "SELECT F_ID FROM source_table");
        options.put("doris.read.field", "F_ID");

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> DorisTableConfig.of(ReadonlyConfig.fromMap(options)));
        Assertions.assertTrue(error.getMessage().contains("doris.read.field"));
    }

    private Map<String, Object> baseOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("database", "source_db");
        options.put("table", "source_table");
        return options;
    }
}
