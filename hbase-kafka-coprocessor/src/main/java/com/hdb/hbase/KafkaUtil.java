package com.hdb.hbase;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaUtil {
    public static String topic = "hbase2kafka";//定义主题

    public static void sendMsg(String servers, String topic, String msg) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);//kafka地址，多个地址用逗号分割
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
            kafkaProducer.send(record);
            System.out.println("消息发送成功:" + msg);
        } finally {
            kafkaProducer.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
//        Properties p = new Properties();
//        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.101.40.206:9092,10.101.40.207:9092,10.101.40.208:9092");//kafka地址，多个地址用逗号分割
//        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);
//        try {
//            while (true) {
//                String msg = "Hello," + new Random().nextInt(100);
//                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
//                kafkaProducer.send(record);
//                System.out.println("消息发送成功:" + msg);
//                Thread.sleep(1000);
//            }
//        } finally {
//            kafkaProducer.close();
//        }

        sendMsg("10.101.40.206:9092,10.101.40.207:9092,10.101.40.208:9092", "hbase2kafka", "hello world");
    }
}
