package org.baozi;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallback {
    public static void main(String[] args) {
        // 配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.21.0.15:9093");
        // 制定key和value的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class);

        // 创建kafka对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 发送消息
//        for (int i = 0; i < 10; i++)
//            kafkaProducer.send(new ProducerRecord<>("first", "baozi" + i), (metadata, exception) -> {
//                if (exception ==null)
//                    System.out.println("主题：" + metadata.topic() + " 分区：" + metadata.partition());
//            });

        kafkaProducer.send(new ProducerRecord<>("first", "baozi"), (metadata, exception) -> {
            if (exception == null)
                System.out.println("主题：" + metadata.topic() + " 分区：" + metadata.partition()) ;
        });
        kafkaProducer.send(new ProducerRecord<>("first", "test"), (metadata, exception) -> {
            if (exception == null)
                System.out.println("主题：" + metadata.topic() + " 分区：" + metadata.partition()) ;
        });
        // 关闭资源
        kafkaProducer.close();
    }
}
