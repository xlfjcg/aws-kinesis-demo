package com.starrocks.demo.flink.entity;

import com.starrocks.connector.flink.table.data.StarRocksRowData;

public class DefaultStarRocksRowData implements StarRocksRowData {

    private String database;
    private String table;
    private String row;
    private String keyBy;

    public String getKeyBy() {
        return keyBy;
    }

    @Override
    public String getUniqueKey() {
        return database + "." + table;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public String getRow() {
        return row;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setRow(String row) {
        this.row = row;
    }

    public void setKeyBy(String keyBy) {
        this.keyBy = keyBy;
    }
}
