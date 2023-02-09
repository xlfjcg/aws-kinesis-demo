package com.starrocks.demo.flink.util;

public class StarRocksUtils {

    public static String getTableKey(String database, String table) {
        return database + "." + table;
    }
}
