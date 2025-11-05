package org.daodao.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.daodao.kafka.util.Constants;

import java.util.Properties;

@Slf4j
public class KafkaMsgProducer {
    public static void main(String[] args) {
        sendMessage(Constants.TOPIC, "Hello, Kafka!");
    }

    public static void sendMessage(String topicName, String message) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topicName, message));
            log.info("Message sent: " + message);
        }
    }


}