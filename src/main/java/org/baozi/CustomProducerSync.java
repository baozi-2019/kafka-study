package org.baozi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomProducerSync {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.21.0.15:9093");
        // 制定key和value的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 创建kafka对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 发送消息
        for (int i = 0; i < 10; i++)
            kafkaProducer.send(new ProducerRecord<>("first", "baozi" + i)).get();
        // 关闭资源
        kafkaProducer.close();
    }
}
