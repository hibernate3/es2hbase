package com.hdb.es;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hdb.es.util.ESUtil;
import com.hdb.es.util.KafkaReceiverListener;
import com.hdb.es.util.KafkaUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

public class Kafka2ES {
    public static void main(String[] args) {
        KafkaUtil.receiveMsg("10.101.40.206:9092,10.101.40.207:9092,10.101.40.208:9092", "hbase2kafka", "es_sink_group", 100, new KafkaReceiverListener() {
            @Override
            public void onReceive(String topic, long offset, String msg) {
                JsonObject jsonObject = (JsonObject) new JsonParser().parse(msg);
                String type = jsonObject.get("type").getAsString();
                String namespace = jsonObject.get("namespace").getAsString();//HBase namespace
                String tableName = jsonObject.get("tableName").getAsString();//HBase table

                if (namespace.equals("HDB") && tableName.equals("HDB:SHARE_BUILDING_DETAILS")) {
                    if (type.equals("put")) {//新增或者更新操作
                        String rowKey = jsonObject.get("rowKey").getAsString();
                        JsonArray columnFamilies = jsonObject.get("columnFamilies").getAsJsonArray();

                        //解析列族
                        for (int i = 0; i < columnFamilies.size(); i++) {
                            JsonObject cfJson = columnFamilies.get(i).getAsJsonObject();

                            if (cfJson.get("_0").getAsString().equals("INFO")) {
                                String brokerId = cfJson.get("BROKER_ID")==null?"":cfJson.get("BROKER_ID").getAsString();
                                String buildingId = cfJson.get("BUILDING_ID")==null?"":cfJson.get("BUILDING_ID").getAsString();
                                String wxOpenId = cfJson.get("WX_OPEN_ID")==null?"":cfJson.get("WX_OPEN_ID").getAsString();
                                String wxUnionId = cfJson.get("WX_UNION_ID")==null?"":cfJson.get("WX_UNION_ID").getAsString();
                                Long timestamp = Long.parseLong(cfJson.get("TIMESTAMP")==null?"0":cfJson.get("TIMESTAMP").getAsString());

                                //时间转换
                                Date dat = new Date(timestamp);
                                GregorianCalendar gc = new GregorianCalendar();
                                gc.setTime(dat);
                                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                String dateTime = format.format(gc.getTime());

                                //es doc json拼装
                                Map<String, Object> jsonMap = new HashMap<String, Object>();
                                jsonMap.put("row_key", rowKey);
                                jsonMap.put("broker_id", brokerId);
                                jsonMap.put("building_id", buildingId);
                                jsonMap.put("wx_open_id", wxOpenId);
                                jsonMap.put("wx_union_id", wxUnionId);
                                jsonMap.put("date_time", dateTime);

                                String esDocJson = new Gson().toJson(jsonMap);

                                System.out.println(esDocJson);

                                ESUtil.createDoc("hdb_share_building_details_index", esDocJson, rowKey);
                            }
                        }
                    } else {//删除操作
                        String rowKey = jsonObject.get("rowKey").getAsString();
                        ESUtil.deleteDoc("hdb_share_building_details_index", rowKey);
                    }
                }
            }
        });
    }
}