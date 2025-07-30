package com.piyush.clickhousefileintegration.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.stereotype.Service;

import com.piyush.clickhousefileintegration.model.ClickHouseConfig;
import com.piyush.clickhousefileintegration.model.ColumnMetadata;
import com.piyush.clickhousefileintegration.model.FlatFileConfig;
import com.piyush.clickhousefileintegration.model.IngestionRequest;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class IntegrationService {

    private final ClickHouseService clickHouseService;

    private final FlatFileService flatFileService;

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

    /**
     * Fetches the schema of a flat file
     *
     * @param config Flat file configuration
     * @return List of column metadata
     * @throws IOException          if file operation fails
     * @throws InterruptedException
     */
    public List<ColumnMetadata> getFlatFileSchema(FlatFileConfig config) throws IOException, InterruptedException {
        return flatFileService.readFileSchema(config);
    }

    /**
     * Previews data from a flat file
     *
     * @param config  Flat file configuration
     * @param columns List of columns to preview
     * @param limit   Maximum number of rows to preview
     * @return List of data rows
     * @throws IOException          if file operation fails
     * @throws InterruptedException
     */
    public List<Map<String, Object>> previewFlatFileData(FlatFileConfig config, List<ColumnMetadata> columns,
            int limit) throws IOException, InterruptedException {
        return flatFileService.readData(config, columns, limit);
    }

    /**
     * Previews data from ClickHouse with JOIN
     *
     * @param config           ClickHouse configuration
     * @param mainTable        Main table name
     * @param additionalTables Additional tables for JOIN
     * @param joinCondition    JOIN condition
     * @param columns          List of columns to preview
     * @param limit            Maximum number of rows to preview
     * @return List of data rows
     * @throws SQLException if database operation fails
     */
    public List<Map<String, Object>> previewClickHouseJoinData(ClickHouseConfig config, String mainTable,
            List<String> additionalTables, String joinCondition,
            List<ColumnMetadata> columns, int limit) throws SQLException {
        try (Connection connection = clickHouseService.connect(config)) {
            return clickHouseService.previewJoinData(connection, mainTable, additionalTables, joinCondition, columns,
                    limit);
        }
    }

    /**
     * Ingests data from a flat file into a ClickHouse table.
     *
     * The method performs the following steps:
     * - Establishes a connection to ClickHouse using the provided configuration
     * - Creates the target table if it doesn't already exist
     * - Reads data from the flat file based on selected columns
     * - Inserts the data into the ClickHouse table using batch insert
     *
     * @param request The ingestion request containing source and target
     *                configurations
     * @return The number of records successfully inserted into ClickHouse
     * @throws SQLException If a database error occurs during the process
     * @throws IOException  If a file I/O error occurs while reading the flat file
     */
    private int ingestFromFlatFileToClickHouse(IngestionRequest request)
            throws SQLException, IOException, InterruptedException {
        log.info("Ingesting data from Flat File to ClickHouse");

        try (Connection connection = clickHouseService.connect(request.getClickHouseConfig())) {
            // Create target table in ClickHouse if it doesn't exist
            clickHouseService.createTable(connection, request.getTargetTableName(), request.getSelectedColumns());

            // Read data from flat file
            List<Map<String, Object>> data = flatFileService.readData(
                    request.getFlatFileConfig(),
                    request.getSelectedColumns(),
                    0); // No limit for full ingestion

            // Insert data into ClickHouse
            int recordCount = clickHouseService.insertData(
                    connection,
                    request.getTargetTableName(),
                    request.getSelectedColumns(),
                    data);

            log.info("Ingestion completed: {} records transferred from Flat File to ClickHouse", recordCount);
            return recordCount;
        }
    }

    /**
     * Executes the ingestion process between source and target based on the given
     * request.
     *
     * @param request the ingestion request containing source, target, and selected
     *                columns
     * @return the number of records processed
     * @throws Exception if the ingestion process fails
     */
    public int executeIngestion(IngestionRequest request) throws Exception {
        log.info("Initiating ingestion: Source [{}], Target [{}]", request.getSourceType(), request.getTargetType());

        validateRequest(request);

        String source = request.getSourceType().toLowerCase();
        String target = request.getTargetType().toLowerCase();

        if ("clickhouse".equals(source) && "flatfile".equals(target)) {
            return ingestFromClickHouseToFlatFile(request);
        } else if ("flatfile".equals(source) && "clickhouse".equals(target)) {
            return ingestFromFlatFileToClickHouse(request);
        } else {
            throw new IllegalArgumentException(String.format(
                    "Ingestion from [%s] to [%s] is not supported", request.getSourceType(), request.getTargetType()));
        }
    }

    /**
     * Validates the ingestion request for completeness and correctness.
     *
     * @param request the ingestion request to be validated
     * @throws IllegalArgumentException if the request contains invalid or missing
     *                                  fields
     */
    private void validateRequest(IngestionRequest request) {
        if (request.getSourceType() == null || request.getTargetType() == null) {
            throw new IllegalArgumentException("Both sourceType and targetType must be specified");
        }

        switch (request.getSourceType().toLowerCase()) {
            case "clickhouse":
                if (request.getClickHouseConfig() == null) {
                    throw new IllegalArgumentException("ClickHouse configuration must be provided as the source");
                }
                break;
            case "flatfile":
                if (request.getFlatFileConfig() == null) {
                    throw new IllegalArgumentException("FlatFile configuration must be provided as the source");
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported sourceType: " + request.getSourceType());
        }

        switch (request.getTargetType().toLowerCase()) {
            case "clickhouse":
                if (request.getClickHouseConfig() == null) {
                    throw new IllegalArgumentException("ClickHouse configuration must be provided as the target");
                }
                break;
            case "flatfile":
                if (request.getFlatFileConfig() == null) {
                    throw new IllegalArgumentException("FlatFile configuration must be provided as the target");
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported targetType: " + request.getTargetType());
        }

        if (request.getSelectedColumns() == null || request.getSelectedColumns().isEmpty()) {
            throw new IllegalArgumentException("At least one column must be selected for ingestion");
        }
    }

    /**
     * Performs data ingestion from ClickHouse to a flat file.
     *
     * @param request the ingestion request containing source and destination
     *                configurations
     * @return the total number of records successfully processed
     * @throws SQLException if an error occurs during ClickHouse database operations
     * @throws IOException  if an error occurs during file writing operations
     */
    private int ingestFromClickHouseToFlatFile(IngestionRequest request) throws SQLException, IOException {
        Objects.requireNonNull(request, "Ingestion request must not be null");
        log.info("Initiating data ingestion from ClickHouse to Flat File...");

        try (Connection connection = clickHouseService.connect(request.getClickHouseConfig())) {
            ClickHouseService.DataHandler flatFileHandler = flatFileService
                    .createFlatFileDataHandler(request.getFlatFileConfig(), request.getSelectedColumns());

            int recordCount;
            boolean isJoinRequired = request.getAdditionalTables() != null && !request.getAdditionalTables().isEmpty()
                    && request.getJoinCondition() != null && !request.getJoinCondition().isEmpty();

            if (isJoinRequired) {
                log.info("Executing JOIN-based ingestion with additional tables: {}", request.getAdditionalTables());
                recordCount = clickHouseService.transferJoinDataFromClickHouse(
                        connection,
                        request.getTableName(),
                        request.getAdditionalTables(),
                        request.getJoinCondition(),
                        request.getSelectedColumns(),
                        flatFileHandler);
            } else {
                log.info("Executing simple ingestion for table: {}", request.getTableName());
                recordCount = clickHouseService.transferDataFromClickHouse(
                        connection,
                        request.getTableName(),
                        request.getSelectedColumns(),
                        flatFileHandler);
            }

            log.info("Ingestion completed successfully. Total records transferred: {}", recordCount);
            return recordCount;
        }
    }

}
