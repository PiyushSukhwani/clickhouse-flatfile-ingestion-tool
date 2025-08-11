package com.piyush.clickhousefileintegration.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.piyush.clickhousefileintegration.model.ColumnMetadata;
import com.piyush.clickhousefileintegration.model.FlatFileConfig;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class FlatFileService {

    /**
     * Resolves a file path or URL to a local file path
     * 
     * @param filePathOrUrl File path or URL string
     * @return Local file path
     * @throws InterruptedException
     * @throws IOException          if file cannot be accessed or downloaded
     */
    private String resolveFilePathOrUrl(String filePathOrUrl) throws IOException, InterruptedException {
        if (filePathOrUrl == null || filePathOrUrl.trim().isEmpty()) {
            throw new IOException("File path or URL cannot be empty");
        }

        // Check if the file path is a temporary file that already exists
        // This handles cases where the system has already downloaded the file to a temp
        // location
        if (filePathOrUrl.contains("/var/folders/") || filePathOrUrl.contains("temp_") ||
                filePathOrUrl.startsWith(System.getProperty("java.io.tmpdir"))) {
            Path tempFilePath = Paths.get(filePathOrUrl);
            if (Files.exists(tempFilePath)) {
                log.info("Using existing temporary file: {}", filePathOrUrl);
                return filePathOrUrl;
            }
        }

        // Check if the input is a URL
        if (filePathOrUrl.toLowerCase().startsWith("http://") || filePathOrUrl.toLowerCase().startsWith("https://")) {
            log.info("Detected URL: {}", filePathOrUrl);
            try {
                // Create a HttpClient object to make connection
                HttpClient client = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .build();

                // Set user agent to avoid potential blocking
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(filePathOrUrl))
                        .timeout(Duration.ofSeconds(30))
                        .header("User-Agent", "Clickhouse-flatfile-Integration-Tool/1.0")
                        .GET()
                        .build();

                HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());

                // Check response code
                if (response.statusCode() != 200) {
                    throw new IOException("Failed to get schema: HTTP error code: " + response.statusCode());
                }

                // Create a temporary file to store the downloaded content
                String tempFileName = "temp_" + UUID.randomUUID().toString() + ".csv";
                File tempFile = new File(System.getProperty("java.io.tmpdir"), tempFileName);

                // Write the downloaded content to the file
                try (InputStream inputStream = response.body()) {
                    Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }

                log.info("Downloaded URL content to temporary file: {}", tempFile.getAbsolutePath());
                return tempFile.getAbsolutePath();

            } catch (MalformedURLException e) {
                throw new IOException("Invalid URL format: " + e.getMessage(), e);
            } catch (IOException e) {
                throw new IOException("Error downloading file from URL: " + e.getMessage(), e);
            }
        } else {
            // It's a local file path, verify it exists
            Path filePath = Paths.get(filePathOrUrl);
            if (!Files.exists(filePath)) {
                throw new IOException("File not found: " + filePathOrUrl);
            }
            log.info("Downloading file from the user's system: " + filePathOrUrl);
            return filePathOrUrl;
        }
    }

    /**
     * Reads the schema (column names and inferred types) from a flat file
     *
     * @param config Flat file configuration
     * @return List of column metadata
     * @throws IOException          if file reading fails
     * @throws InterruptedException
     */
    public List<ColumnMetadata> readFileSchema(FlatFileConfig config, MultipartFile file)
            throws IOException, InterruptedException {
        // validateConfig(config);

        try (Reader reader = createReader(config, file)) {
            CSVFormat csvFormat = buildCsvFormat(config);
            return parseColumns(reader, csvFormat, config.isHasHeader());
        }
    }

    private Reader createReader(FlatFileConfig config, MultipartFile file) throws IOException, InterruptedException {
        if (file != null && !file.isEmpty()) {
            log.info("Reading file directly from uploaded MultipartFile: {}", file.getOriginalFilename());
            return new BufferedReader(
                    new InputStreamReader(file.getInputStream(), Charset.forName(config.getEncoding())));
        } else {
            String resolvedPath = resolveFilePathOrUrl(config.getFileName());
            log.info("Reading file from resolved path/URL: {}", resolvedPath);
            return new BufferedReader(
                    new InputStreamReader(new FileInputStream(resolvedPath), Charset.forName(config.getEncoding())));
        }
    }

    private CSVFormat buildCsvFormat(FlatFileConfig config) {
        CSVFormat.Builder builder = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                .setDelimiter(config.getDelimiter().charAt(0))
                .setSkipHeaderRecord(config.isHasHeader());

        if (config.isHasHeader()) {
            builder.setHeader(); // Auto-detect header from file
        }
        return builder.build();
    }

    private List<ColumnMetadata> parseColumns(Reader reader, CSVFormat csvFormat, boolean hasHeader)
            throws IOException {
        List<ColumnMetadata> columns = new ArrayList<>();

        try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
            if (hasHeader) {
                Map<String, Integer> headerMap = csvParser.getHeaderMap();
                for (String header : headerMap.keySet()) {
                    columns.add(new ColumnMetadata(header, ""));
                }

                if (csvParser.iterator().hasNext()) {
                    CSVRecord record = csvParser.iterator().next();
                    for (int j = 0; j < headerMap.size(); j++) {
                        String value = record.get(j);
                        columns.get(j).setType(inferType(value));
                    }
                }
            } else {
                if (csvParser.iterator().hasNext()) {
                    CSVRecord record = csvParser.iterator().next();
                    for (int i = 0; i < record.size(); i++) {
                        columns.add(new ColumnMetadata("Column_" + (i + 1), inferType(record.get(i))));
                    }
                }
            }
        }

        return columns;
    }

    /**
     * Infers the data type of a value
     *
     * @param value String value to analyze
     * @return Inferred type name
     */
    private String inferType(String value) {
        if (value == null || value.isEmpty()) {
            return "String";
        }

        // Try to parse as integer
        try {
            Long.parseLong(value);
            return "Integer";
        } catch (NumberFormatException e) {
            // Not an integer
        }

        // Try to parse as double
        try {
            Double.parseDouble(value);
            return "Double";
        } catch (NumberFormatException e) {
            // Not a double
        }

        // Try to parse as date (simplified check)
        if (value.matches("\\d{4}-\\d{2}-\\d{2}") ||
                value.matches("\\d{2}/\\d{2}/\\d{4}") ||
                value.matches("\\d{2}-\\d{2}-\\d{4}")) {
            return "Date";
        }

        // Try to parse as boolean
        String lowerValue = value.toLowerCase();
        if (lowerValue.equals("true") || lowerValue.equals("false") ||
                lowerValue.equals("yes") || lowerValue.equals("no") ||
                lowerValue.equals("1") || lowerValue.equals("0")) {
            return "Boolean";
        }

        // Default to string
        return "String";
    }

    /**
     * Reads data from a flat file
     *
     * @param config  Flat file configuration
     * @param columns List of columns to read
     * @param limit   Maximum number of rows to read (for preview)
     * @return List of maps representing rows of data
     * @throws IOException          if file reading fails
     * @throws InterruptedException
     */
    public List<Map<String, Object>> readData(FlatFileConfig config,
            MultipartFile file,
            List<ColumnMetadata> columns,
            int limit)
            throws IOException, InterruptedException {

        List<Map<String, Object>> results = new ArrayList<>();

        // Validate config
        if (config == null) {
            throw new IOException("FlatFileConfig cannot be null");
        }

        // Validate columns
        if (columns == null || columns.isEmpty()) {
            log.warn("Column list is null or empty, nothing to preview");
            return results;
        }

        // Get selected column names
        List<String> selectedColumnNames = getSelectedColumnNames(columns);
        if (selectedColumnNames.isEmpty()) {
            log.warn("No valid columns available to preview after selection");
            return results;
        }

        // Determine file input source
        Reader reader = createReader(config, file);

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(config.isHasHeader())
                .setIgnoreHeaderCase(true)
                .setAllowMissingColumnNames(true)
                .setDelimiter(config.getDelimiter().charAt(0))
                .build();

        try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
            int rowCount = 0;
            Map<String, Integer> headerMap = csvParser.getHeaderMap();
            log.info("CSV header map: {}", headerMap);

            for (CSVRecord record : csvParser) {
                if (limit > 0 && rowCount >= limit)
                    break;

                Map<String, Object> row = config.isHasHeader()
                        ? readRowUsingHeader(record, selectedColumnNames, headerMap)
                        : readRowUsingIndex(record, selectedColumnNames);

                results.add(row);
                rowCount++;
            }

            log.info("Read {} records from file", rowCount);

        } catch (Exception e) {
            log.error("Error reading data from file: {}", e.getMessage(), e);
            throw new IOException("Error reading data from file: " + e.getMessage());
        }

        return results;
    }

    private List<String> getSelectedColumnNames(List<ColumnMetadata> columns) {
        List<String> selected = columns.stream()
                .filter(col -> col != null && col.isSelected() && col.getName() != null).map(ColumnMetadata::getName)
                .collect(Collectors.toList());

        // Auto-select all columns if none were explicitly selected
        if (selected.isEmpty()) {
            columns.stream()
                    .filter(column -> column != null && column.getName() != null)
                    .peek(column -> column.setSelected(true)) // side-effect like in your original loop
                    .map(ColumnMetadata::getName)
                    .forEach(selected::add);
        }

        return selected;
    }

    /**
     * Constructs a row map from a CSV record using custom or undefined column names
     * (by index).
     *
     * This method assumes the CSV has no headers and maps each value in the record
     * to the corresponding
     * column name in the provided list. If the record has fewer fields than column
     * names, remaining
     * values are filled with empty strings.
     *
     * @param record      The CSV record to extract data from.
     * @param columnNames List of column names to map values to (ordered by
     *                    position).
     * @return A map representing a row with column names as keys and record values
     *         as values.
     */

    private Map<String, Object> readRowUsingIndex(CSVRecord record, List<String> columnNames) {
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String value = i < record.size() ? record.get(i) : "";
            row.put(columnNames.get(i), value);
        }
        return row;
    }

    /**
     * Constructs a row map from a CSV record using defined column names from the
     * header.
     *
     * This method attempts to retrieve values based on the provided column names.
     * If a column name
     * does not directly match a header in the CSV, a case-insensitive match is
     * attempted.
     * If no match is found, the value is set as an empty string.
     *
     * @param record      The CSV record to extract data from.
     * @param columnNames List of expected column names.
     * @param headerMap   The header map from the CSV parser for name lookup.
     * @return A map representing a row with column names as keys and corresponding
     *         record values.
     */

    private Map<String, Object> readRowUsingHeader(CSVRecord record, List<String> columnNames,
            Map<String, Integer> headerMap) {
        Map<String, Object> row = new HashMap<>();

        for (String columnName : columnNames) {
            try {
                String value = record.get(columnName);
                row.put(columnName, value);
            } catch (IllegalArgumentException e) {
                // Try case-insensitive match
                boolean found = false;
                for (String header : headerMap.keySet()) {
                    if (header.equalsIgnoreCase(columnName)) {
                        row.put(columnName, record.get(header));
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    log.warn("Column '{}' not found in CSV header", columnName);
                    row.put(columnName, "");
                }
            }
        }

        return row;
    }

    /**
     * Creates a DataHandler for writing to a flat file.
     *
     * @param config  Flat file configuration
     * @param columns List of columns to write
     * @return DataHandler for writing to the flat file
     */
    public ClickHouseService.DataHandler createFlatFileDataHandler(FlatFileConfig config,
            List<ColumnMetadata> columns, AtomicReference<File> generatedFileRef, String tableName) {

        List<String> selectedColumnNames = Optional.ofNullable(columns)
                .map(list -> list.stream()
                        .filter(Objects::nonNull)
                        .filter(ColumnMetadata::isSelected)
                        .map(ColumnMetadata::getName)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()))
                .orElseGet(() -> {
                    log.warn("Received null column list; no columns will be processed.");
                    return Collections.emptyList();
                });

        return new ClickHouseService.DataHandler() {
            private CSVPrinter csvPrinter;
            private Writer writer;
            private int recordCount = 0;

            @Override
            public void processRow(Map<String, Object> row) throws SQLException {
                try {
                    if (csvPrinter == null) {
                        log.info("Initializing CSV printer with columns: {}", selectedColumnNames);

                        // ✅ Create a temp file with tableName.csv
                        File tempFile = File.createTempFile(tableName, ".csv");
                        generatedFileRef.set(tempFile);

                        writer = new BufferedWriter(
                                new OutputStreamWriter(
                                        new FileOutputStream(tempFile),
                                        StandardCharsets.UTF_8));

                        CSVFormat format = CSVFormat.DEFAULT.builder()
                                .setDelimiter(config.getDelimiter().charAt(0))
                                .setHeader(selectedColumnNames.toArray(new String[0]))
                                .build();

                        csvPrinter = new CSVPrinter(writer, format);
                    }

                    log.debug("Processing row: {}", row);

                    List<Object> recordValues = new ArrayList<>();
                    for (String columnName : selectedColumnNames) {
                        if (!row.containsKey(columnName)) {
                            log.warn("Missing column '{}'; inserting NULL.", columnName);
                        }
                        recordValues.add(row.get(columnName));
                    }

                    csvPrinter.printRecord(recordValues);
                    recordCount++;

                    if (recordCount % 1000 == 0) {
                        log.info("Written {} records to flat file", recordCount);
                    }
                } catch (IOException e) {
                    throw new SQLException("Failed to write to flat file: " + e.getMessage(), e);
                }
            }

            @Override
            public void complete() throws SQLException {
                try {
                    if (csvPrinter != null) {
                        csvPrinter.flush();
                        csvPrinter.close();
                    }
                    if (writer != null) {
                        writer.close();
                    }
                    log.info("Successfully completed writing {} records to flat file.", recordCount);
                } catch (IOException e) {
                    throw new SQLException("Failed to close flat file resources: " + e.getMessage(), e);
                }
            }
        };
    }
}
