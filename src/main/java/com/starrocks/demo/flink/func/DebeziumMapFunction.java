package com.starrocks.demo.flink.func;

import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.demo.flink.entity.DefaultStarRocksRowData;
import com.starrocks.demo.flink.util.StarRocksUtils;
import com.starrocks.shade.com.alibaba.fastjson.JSON;
import com.starrocks.shade.com.alibaba.fastjson.JSONObject;

import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.TimestampData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DebeziumMapFunction implements MapFunction<String, StarRocksRowData> {
    private static final String PAYLOAD_BEFORE = "before";
    private static final String PAYLOAD_AFTER = "after";
    private static final String PAYLOAD = "payload";
    private static final String PAYLOAD_SOURCE = "source";
    private static final String PAYLOAD_SOURCE_DB = "db";
    private static final String PAYLOAD_SOURCE_TABLE = "table";
    private static final String PAYLOAD_OP = "op";

    private static final String SCHEMA = "schema";
    private static final String SCHEMA_FIELDS = "fields";
    private static final String SCHEMA_FIELD = "field";
    private static final String SCHEMA_FIELD_TYPE = "type";
    private static final String SCHEMA_FIELD_TYPE_STRING = "string";
    private static final String SCHEMA_FIELD_NAME = "name";
    private static final String DEBEZIUM_TIME_PREFIX = "io.debezium.time.";
    private static final String DEBEZIUM_TIME_DATE = "Date";
    private static final String DEBEZIUM_TIME_TIMESTAMP = "Timestamp";
    private static final String DEBEZIUM_TIME_MICRO_TIMESTAMP = "MicroTimestamp";
    private static final String DEBEZIUM_TIME_NANO_TIMESTAMP = "NanoTimestamp";
    private static final String DOT = ".";

    private final Map<String, List<JSONObject>> tableTimeCols = new ConcurrentHashMap<>();
    private Map<String, String[]> tablePrimaryKeysMap = new HashMap<>();

    public DebeziumMapFunction() {

    }

    public DebeziumMapFunction(Map<String, String[]> tablePrimaryKeysMap) {
        this.tablePrimaryKeysMap = tablePrimaryKeysMap;
    }

    @Override
    public StarRocksRowData map(String value) throws Exception {
        JSONObject j = JSON.parseObject(value);
        JSONObject schema = j.getJSONObject(SCHEMA);
        JSONObject payload = j.getJSONObject(PAYLOAD);
        DefaultStarRocksRowData starRocksRowData = new DefaultStarRocksRowData();
        starRocksRowData.setDatabase(payload.getJSONObject(PAYLOAD_SOURCE).getString(PAYLOAD_SOURCE_DB));
        starRocksRowData.setTable(payload.getJSONObject(PAYLOAD_SOURCE).getString(PAYLOAD_SOURCE_TABLE));
        String op = payload.getString(PAYLOAD_OP);
        JSONObject row;
        if (Envelope.Operation.CREATE.code().equals(op) || Envelope.Operation.READ.code().equals(op)) {
            row = transformRow(schema, payload, PAYLOAD_AFTER, starRocksRowData.getDatabase(), starRocksRowData.getTable());
            row.put(StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.UPSERT.ordinal());
            starRocksRowData.setRow(JSON.toJSONString(row));
        } else if (Envelope.Operation.DELETE.code().equals(op)) {
            row = transformRow(schema, payload, PAYLOAD_BEFORE, starRocksRowData.getDatabase(), starRocksRowData.getTable());
            row.put(StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.DELETE.ordinal());
            starRocksRowData.setRow(JSON.toJSONString(row));
        } else {
            row = transformRow(schema, payload, PAYLOAD_AFTER, starRocksRowData.getDatabase(), starRocksRowData.getTable());
            row.put(StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.UPSERT.ordinal());
            starRocksRowData.setRow(JSON.toJSONString(row));
        }

        String tableKey = StarRocksUtils.getTableKey(starRocksRowData.getDatabase(), starRocksRowData.getTable());
        if (tablePrimaryKeysMap.containsKey(tableKey)) {
            StringBuilder keyBySb = new StringBuilder(tableKey);
            for (String pk : tablePrimaryKeysMap.get(tableKey)) {
                keyBySb.append(DOT).append(row.get(pk));
            }
            starRocksRowData.setKeyBy(keyBySb.toString());
        }

        return starRocksRowData;
    }

    private JSONObject transformRow(JSONObject schema, JSONObject payload, String beforeOrAfter, String db, String table) {
        JSONObject row = payload.getJSONObject(beforeOrAfter);
        tableTimeCols.computeIfAbsent(
                StarRocksUtils.getTableKey(db, table),
                k -> schema.getJSONArray(SCHEMA_FIELDS)
                        .parallelStream()
                        .filter(obj ->  beforeOrAfter.equals(((JSONObject)obj).getString(SCHEMA_FIELD)))
                        .flatMap(obj -> ((JSONObject)obj).getJSONArray(SCHEMA_FIELDS).parallelStream().map(o -> (JSONObject)o))
                        .filter(obj -> obj.containsKey(SCHEMA_FIELD_NAME) && obj.getString(SCHEMA_FIELD_NAME).startsWith(DEBEZIUM_TIME_PREFIX) &&  !SCHEMA_FIELD_TYPE_STRING.equals(obj.getString(SCHEMA_FIELD_TYPE)))
                        .collect(Collectors.toList())
        ).forEach(obj -> {
            String fieldName = obj.getString(SCHEMA_FIELD);
            if ( null == row.getLong(fieldName)){
                return; // in case value is null
            }
            switch (obj.getString(SCHEMA_FIELD_NAME).replace(DEBEZIUM_TIME_PREFIX, "")) {
                case DEBEZIUM_TIME_TIMESTAMP:
                    row.put(fieldName, TimestampData.fromEpochMillis(row.getLong(fieldName)).toString());
                    break;
                case DEBEZIUM_TIME_MICRO_TIMESTAMP:
                    long micro = row.getLong(fieldName);
                    row.put(fieldName, TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000)).toString());
                    break;
                case DEBEZIUM_TIME_NANO_TIMESTAMP:
                    long nano = row.getLong(fieldName);
                    row.put(fieldName, TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000)).toString());
                    break;
                case DEBEZIUM_TIME_DATE:
                    row.put(fieldName, TemporalConversions.toLocalDate(row.getLong(fieldName)).toString());
                    break;
            }
        });
        return row;
    }
}
