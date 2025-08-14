package com.piyush.clickhousefileintegration.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import com.piyush.clickhousefileintegration.model.ClickHouseConfig;
import com.piyush.clickhousefileintegration.model.ColumnMetadata;
import com.piyush.clickhousefileintegration.model.FlatFileConfig;
import com.piyush.clickhousefileintegration.model.IngestionRequest;
import com.piyush.clickhousefileintegration.service.IntegrationService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@RestController
@RequestMapping("/api/integration")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "https://clickhouse-flatfile-ingestion-tool.vercel.app/")
public class IntegrationController {

    private final IntegrationService integrationService;

    @PostMapping("/clickhouse/test-connection")
    public ResponseEntity<Map<String, Object>> testClickHouseConnection(@RequestBody ClickHouseConfig config) {

        Map<String, Object> response = new HashMap<>();

        try {
            integrationService.getClickHouseTables(config);
            response.put("Success", true);
            response.put("message", "Connection Successful");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error connecting to ClickHouse", e);
            response.put("success", false);
            response.put("message", "Connection failed: " + e.getMessage());
            return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
        }
    }

    /**
     * Gets the list of tables from ClickHouse
     *
     * @param config ClickHouse connection configuration
     * @return List of table names
     */
    @PostMapping("/clickhouse/tables")
    public ResponseEntity<Map<String, Object>> getClickHouseTables(@RequestBody ClickHouseConfig config) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<String> tables = integrationService.getClickHouseTables(config);
            response.put("success", true);
            response.put("tables", tables);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting ClickHouse tables", e);
            response.put("success", false);
            response.put("message", "Failed to get tables: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Gets the schema of a ClickHouse table
     *
     * @param config    ClickHouse connection configuration
     * @param tableName Table name
     * @return List of column metadata
     */
    @PostMapping("/clickhouse/schema")
    public ResponseEntity<Map<String, Object>> getClickHouseSchema(
            @RequestBody ClickHouseConfig config,
            @RequestParam String tableName) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<ColumnMetadata> columns = integrationService.getClickHouseTableSchema(config, tableName);
            response.put("success", true);
            response.put("columns", columns);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting ClickHouse schema", e);
            response.put("success", false);
            response.put("message", "Failed to get schema: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @PostMapping("/clickhouse/preview")
    public ResponseEntity<Map<String, Object>> previewClickHouseData(@RequestBody IngestionRequest request) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<Map<String, Object>> data = null;
            if (request.getAdditionalTables() != null && !request.getAdditionalTables().isEmpty() &&
                    request.getJoinCondition() != null && !request.getJoinCondition().isEmpty()) {
                // Use JOIN preview if multiple tables are selected
                data = integrationService.previewClickHouseJoinData(
                        request.getClickHouseConfig(),
                        request.getTableName(),
                        request.getAdditionalTables(),
                        request.getJoinCondition(),
                        request.getSelectedColumns(),
                        100); // Preview limit
            } else {
                // Use simple preview for single table
                data = integrationService.previewClickHouseData(
                        request.getClickHouseConfig(),
                        request.getTableName(),
                        request.getSelectedColumns(),
                        100); // Preview limit
            }
            response.put("success", true);
            response.put("data", data);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error previewing ClickHouse data", e);
            response.put("success", false);
            response.put("message", "Failed to preview data: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Gets the schema of a flat file
     *
     * @param config Flat file configuration
     * @return List of column metadata
     * @throws Exception
     */
    @PostMapping(value = "/flatfile/schema", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> getFlatFileSchema(
            @RequestPart("flatFileConfig") FlatFileConfig config,
            @RequestPart(value = "file", required = false) MultipartFile file) {

        boolean hasFileUrl = config.getFileName() != null && !config.getFileName().isEmpty();
        boolean hasFile = file != null && !file.isEmpty();

        if (!hasFileUrl && !hasFile) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Either a file URL (fileName) or an uploaded file must be provided.");
        }
        Map<String, Object> response = new HashMap<>();
        try {
            List<ColumnMetadata> columns = integrationService.getFlatFileSchema(config, file);
            response.put("success", true);
            response.put("columns", columns);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting flat file schema", e);
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Failed to preview data: " + e.getMessage(),
                    e);
        }
    }

    /**
     * Previews data from a flat file
     *
     * @param request Ingestion request with source configuration and column
     *                selection
     * @return Preview data
     */
    @PostMapping(value = "/flatfile/preview", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> previewFlatFileData(
            @RequestPart("ingestionRequest") IngestionRequest request,
            @RequestPart(value = "file", required = false) MultipartFile file) {

        boolean hasFileUrl = request.getFlatFileConfig().getFileName() != null
                && !request.getFlatFileConfig().getFileName().isEmpty();
        boolean hasFile = file != null && !file.isEmpty();

        if (!hasFileUrl && !hasFile) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Either a file URL (fileName) or an uploaded file must be provided.");
        }

        try {
            // Log the request for debugging
            log.info("Received preview request for flat file: {}", request.getFlatFileConfig());
            log.info("Selected columns: {}", request.getSelectedColumns());

            List<Map<String, Object>> data = integrationService.previewFlatFileData(
                    request.getFlatFileConfig(),
                    file,
                    request.getSelectedColumns(),
                    100 // Preview limit
            );

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("data", data);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error previewing flat file data", e);
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Failed to preview data: " + e.getMessage(),
                    e);
        }
    }

    /**
     * Executes the data ingestion process
     *
     * @param request Ingestion request with source, target, and column selection
     * @return Ingestion result with record count
     * @throws IOException
     * @throws SQLException
     * @throws InterruptedException
     */
    @PostMapping(value = "/execute", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> executeIngestion(
            @RequestPart("ingestionRequest") IngestionRequest request,
            @RequestPart(value = "file", required = false) MultipartFile file)
            throws SQLException, IOException, InterruptedException {

        try {
            integrationService.validateRequest(request);

            String source = request.getSourceType().toLowerCase();
            String target = request.getTargetType().toLowerCase();

            if ("clickhouse".equals(source) && "flatfile".equals(target)) {

                // ClickHouse → Flatfile: return file
                AtomicReference<File> generatedFileRef = new AtomicReference<>();
                int recordCount = integrationService.ingestFromClickHouseToFlatFile(request, generatedFileRef);

                File generatedFile = generatedFileRef.get();
                if (generatedFile == null || !generatedFile.exists()) {
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
                }

                InputStreamResource resource = new InputStreamResource(new FileInputStream(generatedFile));

                return ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_DISPOSITION,
                                "attachment; filename=\"" + generatedFile.getName() + "\"")
                        .header("X-Record-Count", String.valueOf(recordCount))
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .contentLength(generatedFile.length())
                        .body(resource);

            } else if ("flatfile".equals(source) && "clickhouse".equals(target)) {

                // Flatfile → ClickHouse: return record count
                int recordCount = integrationService.ingestFromFlatFileToClickHouse(request, file);
                return ResponseEntity.ok(recordCount);
            } else {
                return ResponseEntity.badRequest()
                        .body("Ingestion from " + request.getSourceType() + " to " + request.getTargetType()
                                + " is not supported.");
            }
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Ingestion failed: " + e.getMessage());
        }
    }
}
