package com.piyush.clickhousefileintegration.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.springframework.stereotype.Service;

import com.piyush.clickhousefileintegration.model.ClickHouseConfig;
import com.piyush.clickhousefileintegration.model.ColumnMetadata;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ClickHouseService {

    /**
     * Establishes a connectin to ClickHouse using provided configuration
     * 
     * @param config ClickHouse Connection Configuration
     * @return Connection object
     * @throws SQLException if connection fails
     */
    public Connection connect(ClickHouseConfig config) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.getUser());

        if (config.getJwtToken() != null && !config.getJwtToken().isEmpty()) {
            props.setProperty("password", config.getJwtToken());
            props.setProperty("ssl", String.valueOf(config.isSecure()));
            props.setProperty("use_client_time_zone", "true");
        }

        log.info("Attempting ClickHouse connection with user: {}", config.getUser());
        return DriverManager.getConnection(config.getJdbcUrl(), props);
    }

    /**
     * Retrieves the list of tables in the specified database
     *
     * @param connection ClickHouse connection
     * @return List of table names
     * @throws SQLException if query fails
     */
    public List<String> getTables(Connection connection) throws SQLException {

        List<String> tables = new ArrayList<>();

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW TABLES")) {

            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    /**
     * Retrieves the column metadata for a specific table
     *
     * @param connection ClickHouse connection
     * @param tableName  Table name
     * @return List of column metadata
     * @throws SQLException if query fails
     */
    public List<ColumnMetadata> getTableSchema(Connection connection, String tableName) throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();

        String query = "DESCRIBE TABLE " + tableName;
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                String columnName = rs.getString("name");
                String columnType = rs.getString("type");
                columns.add(new ColumnMetadata(columnName, columnType));
            }
        }

        return columns;
    }

    /**
     * Executes a query to fetch data from ClickHouse
     *
     * @param connection ClickHouse connection
     * @param tableName  Table name
     * @param columns    List of columns to fetch
     * @param limit      Maximum number of rows to fetch (for preview)
     * @return List of maps representing rows of data
     * @throws SQLException if query fails
     */
    public List<Map<String, Object>> previewData(Connection connection, String tableName,
            List<ColumnMetadata> columns, int limit) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();

        // Build column list for SELECT query
        StringBuilder columnList = new StringBuilder();
        for (ColumnMetadata column : columns) {
            if (column.isSelected()) {
                if (columnList.length() > 0) {
                    columnList.append(", ");
                }
                columnList.append("`").append(column.getName()).append("`");
            }
        }

        // If no columns selected, return empty result
        if (columnList.length() == 0) {
            return results;
        }

        String query = String.format("SELECT %s FROM %s LIMIT %d", columnList, tableName, limit);
        log.info("Executing preview query: {}", query);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(columnName, value);
                }
                results.add(row);
            }
        }

        return results;
    }

}
