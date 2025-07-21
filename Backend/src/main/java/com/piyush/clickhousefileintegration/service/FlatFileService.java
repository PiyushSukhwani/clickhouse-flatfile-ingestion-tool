package com.piyush.clickhousefileintegration.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

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
     * @throws IOException if file reading fails
     * @throws InterruptedException 
     */
    public List<ColumnMetadata> readFileSchema(FlatFileConfig config) throws IOException, InterruptedException {
        List<ColumnMetadata> columns = new ArrayList<>();

        // Check if config is null
        if (config == null) {
            log.error("FlatFileConfig is null");
            throw new IOException("FlatFileConfig cannot be null");
        }

        // Check if fileName is null
        if (config.getFileName() == null) {
            log.error("File name is null in FlatFileConfig");
            throw new IOException("File name cannot be null");
        }

        log.info("Attempting to resolve file path: {}", config.getFileName());

        // Resolve file path or URL
        String resolvedFilePath = resolveFilePathOrUrl(config.getFileName());

        try (Reader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(resolvedFilePath), Charset.forName(config.getEncoding())))) {
            // Configure CSV parser based on the delimiter
            // Deprecated
            // CSVFormat csvFormat = CSVFormat.DEFAULT
            // .withDelimiter(config.getDelimiter().charAt(0))
            // .withHeader()
            // .withSkipHeaderRecord(config.isHasHeader());

            CSVFormat.Builder builder = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                    .setDelimiter(config.getDelimiter().charAt(0))
                    .setSkipHeaderRecord(config.isHasHeader());

            if (config.isHasHeader()) {
                builder.setHeader(); // Auto-detect from file
            }

            CSVFormat csvFormat = builder.build();

            try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
                // If file has header, use it for column names
                if (config.isHasHeader()) {
                    Map<String, Integer> headerMap = csvParser.getHeaderMap();
                    for (String header : headerMap.keySet()) {
                        columns.add(new ColumnMetadata(header, ""));
                    }

                    // Try to infer column types from the first record
                    if (csvParser.iterator().hasNext()) {
                        CSVRecord record = csvParser.iterator().next();
                        int i = 0;
                        for (String header : headerMap.keySet()) {
                            String value = record.get(i);
                            String inferredType = inferType(value);
                            columns.get(i).setType(inferredType);
                            i++;
                        }
                    }
                } else {
                    // If no header, create generic column names
                    if (csvParser.iterator().hasNext()) {
                        CSVRecord record = csvParser.iterator().next();
                        for (int i = 0; i < record.size(); i++) {
                            String columnName = "Column_" + (i + 1);
                            String value = record.get(i);
                            String inferredType = inferType(value);
                            columns.add(new ColumnMetadata(columnName, inferredType));
                        }
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

}
