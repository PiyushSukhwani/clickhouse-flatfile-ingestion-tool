package com.piyush.clickhousefileintegration.model;

import lombok.Data;

@Data
public class ClickHouseConfig {

    private String host;
    private int port;
    private String database;
    private String user;
    private String jwtToken;
    
}
