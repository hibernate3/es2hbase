package com.hdb.hbase;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaUtil {
    private static final String kerberosConfigPath = "";

    public static void sendMsg(String servers, String topic, String msg) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);//kafka地址，多个地址用逗号分割
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        System.setProperty("java.security.krb5.conf", kerberosConfigPath + "krb5.conf");
        System.setProperty("java.security.auth.login.config", kerberosConfigPath + "jaas.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        p.put("security.protocol", "SASL_PLAINTEXT");
        p.put("sasl.kerberos.service.name", "kafka");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
            kafkaProducer.send(record);
            System.out.println("消息发送成功:" + msg);
        } finally {
            kafkaProducer.close();
        }
    }
}
