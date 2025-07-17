package com.piyush.clickhousefileintegration.model;

import lombok.Data;

/**
 * Configuration class for Flat File operations
 */
@Data
public class FlatFileConfig {
    private String fileName;
    private String delimiter;
    private boolean hasHeader;
    private String encoding = "UTF-8";
}