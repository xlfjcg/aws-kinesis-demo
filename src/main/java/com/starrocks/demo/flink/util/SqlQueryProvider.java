package com.starrocks.demo.flink.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SqlQueryProvider {
    private final String username;
    private final String password;
    private final String jdbcURL;

    public SqlQueryProvider(String jdbcURL, String username, String password) {
        this.username = username;
        this.password = password;
        this.jdbcURL = jdbcURL;
    }

    public SqlQueryProvider(String host, int port, String username, String password, String... params) {
        String jdbcURL = "jdbc:mysql://" + host + ":" + port;
        if (params != null && params.length > 0) {
            jdbcURL = jdbcURL + "?" + String.join("&", params);
        }

        this.jdbcURL = jdbcURL;
        this.username = username;
        this.password = password;
    }

    public List<Map<String, String>> execute(String sql, Object... args) {
        List<Map<String, String>> columnValues = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(jdbcURL, username, password);
            PreparedStatement ps = conn.prepareStatement(sql)) {
            if (Objects.nonNull(args) && args.length > 0) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Map<String, String> row = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getString(i));
                }
                columnValues.add(row);
            }
            return columnValues;
        } catch (Exception e) {
            throw new RuntimeException(String.format("The following SQL query could not be executed : %s", sql), e);
        }
    }

}
