package com.piyush.clickhousefileintegration.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    /**
     * Creates a ClickHouse table if it does not already exist.
     *
     * The table schema is based on the provided list of selected columns,
     * and column types are mapped to their respective ClickHouse types.
     *
     * The table is created using the MergeTree engine with a default
     * ORDER BY tuple().
     *
     * @param connection Active JDBC connection to ClickHouse
     * @param tableName  Name of the table to be created
     * @param columns    List of column metadata, including names and types
     * @throws SQLException If table creation fails due to SQL error
     */
    public void createTable(Connection connection, String tableName, List<ColumnMetadata> columns) throws SQLException {

        StringBuilder createTableQuery = new StringBuilder();
        createTableQuery.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");

        boolean first = true;
        for (ColumnMetadata column : columns) {
            if (column.isSelected()) {
                if (!first) {
                    createTableQuery.append(", ");
                }
                createTableQuery.append("`").append(column.getName()).append("` ");

                // Map CSV types to ClickHouse types
                String clickHouseType = mapToClickHouseType(column.getType());
                createTableQuery.append(clickHouseType);

                first = false;
            }
        }

        createTableQuery.append(") ENGINE = MergeTree() ORDER BY tuple()");

        String query = createTableQuery.toString();
        log.info("Creating table with query: {}", query);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
        }
    }

    /**
     * Maps a generic or inferred data type to a corresponding ClickHouse data type.
     *
     * The mapping handles common data types such as:
     * - Integer → Int64
     * - Float, Double, or Decimal → Float64
     * - Date or Time → DateTime
     * - Boolean → UInt8
     * - Any unknown or empty type → String
     *
     * @param genericType The inferred or provided generic data type
     * @return The corresponding ClickHouse-compatible data type
     */

    private String mapToClickHouseType(String genericType) {
        // Null or empty check
        if (genericType == null || genericType.trim().isEmpty()) {
            return "String";
        }

        // Normalize input
        String lowerType = genericType.trim().toLowerCase();

        // Type mapping
        if (lowerType.contains("int")) {
            return "Int64";
        } else if (lowerType.contains("float") || lowerType.contains("double") || lowerType.contains("decimal")) {
            return "Float64";
        } else if (lowerType.contains("date") || lowerType.contains("time")) {
            return "DateTime";
        } else if (lowerType.contains("bool")) {
            return "UInt8";
        } else {
            return "String"; // default mapping
        }
    }

    /**
     * Inserts data into a ClickHouse table using batch processing.
     *
     * @param connection ClickHouse database connection
     * @param tableName  Name of the target table
     * @param columns    List of column metadata with selection flags
     * @param data       List of rows to insert, each represented as a map of column
     *                   name to value
     * @return Number of records successfully inserted
     * @throws SQLException if an error occurs during insert execution
     */
    public int insertData(Connection connection, String tableName, List<ColumnMetadata> columns,
            List<Map<String, Object>> data) throws SQLException {

        if (data.isEmpty()) {
            return 0;
        }

        // Build list of selected column names
        List<String> selectedColumnNames = columns.stream()
                .filter(ColumnMetadata::isSelected)
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList());

        if (selectedColumnNames.isEmpty()) {
            return 0;
        }

        // Format column names for SQL (e.g., `ColumnA`, `ColumnB`)
        String columnList = selectedColumnNames.stream()
                .map(name -> "`" + name + "`")
                .collect(Collectors.joining(", "));

        // Generate placeholders (?, ?, ...) for prepared statement
        String placeholders = IntStream.range(0, selectedColumnNames.size())
                .mapToObj(i -> "?")
                .collect(Collectors.joining(", "));

        String insertQuery = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columnList, placeholders);
        System.out.println("This is the insert query string: " + insertQuery);

        // Use batch insert for better performance
        try (PreparedStatement pstmt = connection.prepareStatement(insertQuery)) {
            int batchSize = 10000;
            int count = 0;

            for (Map<String, Object> row : data) {
                int paramIndex = 1;
                for (String columnName : selectedColumnNames) {
                    pstmt.setObject(paramIndex++, row.get(columnName));
                }

                pstmt.addBatch();
                count++;

                if (count % batchSize == 0) {
                    pstmt.executeBatch();
                    log.info("Inserted {} records", count);
                    return count;
                }
            }

            pstmt.executeBatch(); // insert remaining records
            return count;
        }
    }

}
