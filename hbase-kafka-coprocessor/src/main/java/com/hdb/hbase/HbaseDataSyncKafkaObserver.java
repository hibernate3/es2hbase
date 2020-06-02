package com.hdb.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

public class HbaseDataSyncKafkaObserver implements RegionObserver, RegionCoprocessor {
    private static final Logger LOG = Logger.getLogger(HbaseDataSyncKafkaObserver.class);

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

        String indexId = new String(put.getRow());

        try {
            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
                for (Cell cell : entry.getValue()) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));

                    String msg = "key: " + key + ", value: " + value;

                    LOG.info(msg);
                    KafkaUtil.sendMsg("10.101.40.206:9092,10.101.40.207:9092,10.101.40.208:9092", "hbase2kafka", msg);
                }
            }
        } catch (Exception ex) {

        }
    }
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        LOG.info("====Test postDelete====");

        String indexId = new String(delete.getRow());
    }
}
