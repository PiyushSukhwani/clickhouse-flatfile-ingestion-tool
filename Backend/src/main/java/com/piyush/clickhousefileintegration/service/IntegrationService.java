package com.piyush.clickhousefileintegration.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.piyush.clickhousefileintegration.model.ClickHouseConfig;
import com.piyush.clickhousefileintegration.model.ColumnMetadata;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class IntegrationService {

    private final ClickHouseService clickHouseService;

    /**
     * Fetches the list of tables from ClickHouse
     *
     * @param config ClickHouse configuration
     * @return List of table names
     * @throws SQLException if database operation fails
     */
    public List<String> getClickHouseTables(ClickHouseConfig config) throws SQLException {
        try (Connection connection = clickHouseService.connect(config)) {
            return clickHouseService.getTables(connection);
        }
    }

    /**
     * Fetches the schema of a ClickHouse table
     *
     * @param config    ClickHouse configuration
     * @param tableName Table name
     * @return List of column metadata
     * @throws SQLException if database operation fails
     */
    public List<ColumnMetadata> getClickHouseTableSchema(ClickHouseConfig config, String tableName)
            throws SQLException {
        try (Connection connection = clickHouseService.connect(config)) {
            return clickHouseService.getTableSchema(connection, tableName);
        }
    }

    /**
     * Previews data from ClickHouse
     *
     * @param config    ClickHouse configuration
     * @param tableName Table name
     * @param columns   List of columns to preview
     * @param limit     Maximum number of rows to preview
     * @return List of data rows
     * @throws SQLException if database operation fails
     */
    public List<Map<String, Object>> previewClickHouseData(ClickHouseConfig config, String tableName,
            List<ColumnMetadata> columns, int limit) throws SQLException {
        try (Connection connection = clickHouseService.connect(config)) {
            return clickHouseService.previewData(connection, tableName, columns, limit);
        }
    }

}
