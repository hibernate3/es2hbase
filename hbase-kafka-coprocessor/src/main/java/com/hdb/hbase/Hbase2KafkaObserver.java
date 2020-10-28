package com.hdb.hbase;

import com.google.gson.Gson;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class Hbase2KafkaObserver implements RegionObserver, RegionCoprocessor {
    private static final Logger LOG = Logger.getLogger(Hbase2KafkaObserver.class);

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

//    public void start(CoprocessorEnvironment env) throws IOException {
//        LOG.info("====Test Start====");
//    }

//    public void stop(CoprocessorEnvironment env) throws IOException {
//        LOG.info("====Test End====");
//    }

    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        LOG.info("====Test postPut====");

        //表名
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();

        //namespace
        String namespace = e.getEnvironment().getRegion().getRegionInfo().getTable().getNamespaceAsString();

        //rowkey
        String indexId = new String(put.getRow());

        try {
            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
                String columnFamily = new String(entry.getKey());

                Gson gson = new Gson();

                Map<String, Object> jsonMap = new HashMap<String, Object>();
                jsonMap.put("timestamp", System.currentTimeMillis());
                jsonMap.put("namespace", namespace);
                jsonMap.put("tableName", tableName);
                jsonMap.put("rowKey", indexId);
                jsonMap.put("type", "put");

                Map<String, Object> columnFamilyMap = new HashMap<String, Object>();
                List<Object> columnFamilyList = new ArrayList<Object>();

                columnFamilyMap.put("_0", columnFamily);

                for (Cell cell : entry.getValue()) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));

                    columnFamilyMap.put(key, value);
                }

                columnFamilyList.add(columnFamilyMap);
                jsonMap.put("columnFamilies", columnFamilyList);

                String msg = gson.toJson(jsonMap);

                KafkaUtil.sendMsg("10.71.81.222:9092,10.71.81.198:9092,10.71.81.247:9092", "hbase2kafka", msg);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        LOG.info("====Test postDelete====");

        //表名
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();

        //namespace
        String namespace = e.getEnvironment().getRegion().getRegionInfo().getTable().getNamespaceAsString();

        //rowkey
        String indexId = new String(delete.getRow());

        Gson gson = new Gson();

        Map<String, Object> jsonMap = new HashMap<String, Object>();
        jsonMap.put("timestamp", System.currentTimeMillis());
        jsonMap.put("namespace", namespace);
        jsonMap.put("tableName", tableName);
        jsonMap.put("rowKey", indexId);
        jsonMap.put("type", "delete");

        String msg = gson.toJson(jsonMap);

        KafkaUtil.sendMsg("10.71.81.222:9092,10.71.81.198:9092,10.71.81.247:9092", "hbase2kafka", msg);
    }
}
