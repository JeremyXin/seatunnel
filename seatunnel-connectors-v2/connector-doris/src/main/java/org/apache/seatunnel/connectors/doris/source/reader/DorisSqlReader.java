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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;
import org.apache.seatunnel.connectors.doris.util.DorisCatalogUtil;
import org.apache.seatunnel.connectors.doris.util.DorisJdbcRowConverter;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/** Executes a custom Doris query as one bounded source split. */
@Slf4j
final class DorisSqlReader {

    private final DorisSourceConfig config;
    private final DorisSourceTable table;
    private volatile Connection activeConnection;
    private volatile PreparedStatement activeStatement;
    private volatile boolean cancelled;

    DorisSqlReader(DorisSourceConfig config, DorisSourceTable table) {
        this.config = config;
        this.table = table;
    }

    void read(Collector<SeaTunnelRow> output) {
        ensureNotCancelled();
        String jdbcUrl =
                DorisCatalogUtil.getJdbcUrl(
                        DorisCatalogUtil.randomFrontEndHost(config.getFrontends().split(",")),
                        config.getQueryPort(),
                        table.getTablePath().getDatabaseName());
        SeaTunnelRowType rowType = table.getCatalogTable().getSeaTunnelRowType();
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", config.getUsername());
        connectionProperties.setProperty("password", config.getPassword());
        connectionProperties.setProperty(
                "connectTimeout", String.valueOf(config.getRequestConnectTimeoutMs()));
        connectionProperties.setProperty(
                "socketTimeout", String.valueOf(config.getRequestReadTimeoutMs()));
        try (Connection connection = DriverManager.getConnection(jdbcUrl, connectionProperties)) {
            activeConnection = connection;
            ensureNotCancelled();
            try (PreparedStatement statement =
                    connection.prepareStatement(
                            table.getSql(),
                            ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY)) {
                activeStatement = statement;
                ensureNotCancelled();
                statement.setFetchSize(Integer.MIN_VALUE);
                statement.setQueryTimeout(config.getRequestQueryTimeoutS());
                try (ResultSet rows = statement.executeQuery()) {
                    while (true) {
                        ensureNotCancelled();
                        if (!rows.next()) {
                            break;
                        }
                        SeaTunnelRow row = DorisJdbcRowConverter.toRow(rows, rowType);
                        row.setTableId(table.getTablePath().toString());
                        output.collect(row);
                    }
                }
            }
        } catch (SQLException e) {
            if (cancelled) {
                throw new IllegalStateException("Doris SQL reader was cancelled", e);
            }
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.SQL_QUERY_FAILED,
                    "Failed to read SQL result for " + table.getTablePath().getFullName(),
                    e);
        } finally {
            activeStatement = null;
            activeConnection = null;
        }
    }

    void cancel() {
        cancelled = true;
        PreparedStatement statement = activeStatement;
        Connection connection = activeConnection;
        if (statement != null) {
            try {
                statement.cancel();
            } catch (SQLException e) {
                log.warn("Failed to cancel Doris SQL statement", e);
            }
            try {
                statement.close();
            } catch (SQLException e) {
                log.warn("Failed to close cancelled Doris SQL statement", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("Failed to close cancelled Doris SQL connection", e);
            }
        }
    }

    void close() {
        cancel();
    }

    private void ensureNotCancelled() {
        if (cancelled) {
            throw new IllegalStateException("Doris SQL reader was cancelled");
        }
    }
}
