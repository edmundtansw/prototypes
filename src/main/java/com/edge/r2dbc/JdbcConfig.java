package com.edge.r2dbc;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("jdbc")
@Data
public class JdbcConfig {
    private String driverClass;
    private String url;
    private String username;
    private String password;
    private String sessionTable;
    private String validationQuery;
    private int maxIdle;
    private int maxConnections;
}
