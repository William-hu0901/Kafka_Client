package org.daodao.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.daodao.kafka.util.Constants;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
public class KafkaMsgConsumer {

    public static void main(String[] args) {
        consumeMessages(Constants.TOPIC);
    }

    public static void consumeMessages(String topicName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.OFFSET);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> log.info("Received message: {}", record.value()));
            }
        }
    }


}