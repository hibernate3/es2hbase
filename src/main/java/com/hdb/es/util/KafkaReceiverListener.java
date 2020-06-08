package com.hdb.es.util;

public interface KafkaReceiverListener {
    void onReceive(String topic, long offset, String msg);
}
