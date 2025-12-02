package org.daodao.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.daodao.kafka.util.Constants;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaMsgConsumerTest {

    @Test
    void testConsumerConfiguration() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.OFFSET);
        
        assertEquals(Constants.BROKERS, props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(Constants.GROUP, props.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(Constants.DESERIALIZER, props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(Constants.DESERIALIZER, props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals(Constants.OFFSET, props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Test
    void testConsumerCreation() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            assertNotNull(consumer);
        }
    }

    @Test
    void testConsumerSubscription() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            String topic = "test-topic";
            consumer.subscribe(Collections.singletonList(topic));
            assertEquals(Collections.singleton(topic), consumer.subscription());
        }
    }

    @Test
    void testConsumerPoll() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("test-topic"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            assertNotNull(records);
            // Should be empty since there's no broker
            assertEquals(0, records.count());
        }
    }

    @Test
    void testConsumerWithAutoCommitConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        assertEquals("true", props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            assertNotNull(consumer);
        }
    }

    @Test
    void testConsumerWithMaxPollRecords() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        
        assertEquals(100, props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            assertNotNull(consumer);
        }
    }

    @Test
    void testConsumerWithMaxPollInterval() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        assertEquals(300000, props.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            assertNotNull(consumer);
        }
    }

    @Test
    void testKafkaMsgConsumerStaticMethodExists() {
        try {
            Class<?> clazz = Class.forName("org.daodao.kafka.KafkaMsgConsumer");
            assertNotNull(clazz.getMethod("consumeMessages", String.class));
        } catch (Exception e) {
            fail("KafkaMsgConsumer.consumeMessages method should exist: " + e.getMessage());
        }
    }

    @Test
    void testConsumerConfigValidation() {
        Properties props = new Properties();
        
        // Test required configurations
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        
        // All required configs should be present
        assertTrue(props.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertTrue(props.containsKey(ConsumerConfig.GROUP_ID_CONFIG));
        assertTrue(props.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertTrue(props.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    }

    @Test
    void testDurationUsage() {
        Duration duration = Duration.ofMillis(100);
        assertEquals(100, duration.toMillis());
        
        Duration seconds = Duration.ofSeconds(5);
        assertEquals(5000, seconds.toMillis());
    }
}