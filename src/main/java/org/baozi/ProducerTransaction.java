package org.baozi;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerTransaction {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.21.0.15:9093");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 生产者本地发送缓存大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432L);
        // 每批次拉取数据大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 每批次拉取数据大小不足时等待填充时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        // 数据压缩类型
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // 数据分区同步策略
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 事务ID
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "baozi-transaction");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();
        try {
            kafkaProducer.send(new ProducerRecord<>("first", "test1"));
            int x = 1 / 0;
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            kafkaProducer.abortTransaction();
        } finally {
            kafkaProducer.close();
        }
    }
}
