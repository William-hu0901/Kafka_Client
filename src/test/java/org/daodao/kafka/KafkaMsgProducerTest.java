package org.daodao.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.daodao.kafka.util.Constants;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaMsgProducerTest {

    @Test
    void testProducerConfiguration() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        
        assertEquals(Constants.BROKERS, props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(Constants.STR_SERIALIZER, props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(Constants.STR_SERIALIZER, props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void testProducerRecordCreation() {
        String topic = "test-topic";
        String message = "test message";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        
        assertEquals(topic, record.topic());
        assertEquals(message, record.value());
        assertNull(record.key());
    }

    @Test
    void testProducerRecordWithKey() {
        String topic = "test-topic";
        String key = "test-key";
        String message = "test message";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        
        assertEquals(topic, record.topic());
        assertEquals(key, record.key());
        assertEquals(message, record.value());
    }

    @Test
    void testProducerRecordWithPartition() {
        String topic = "test-topic";
        Integer partition = 1;
        String key = "test-key";
        String message = "test message";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, message);
        
        assertEquals(topic, record.topic());
        assertEquals(partition, record.partition());
        assertEquals(key, record.key());
        assertEquals(message, record.value());
    }

    @Test
    void testProducerCreation() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            assertNotNull(producer);
        }
    }

    @Test
    void testProducerWithAcksConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        assertEquals("all", props.get(ProducerConfig.ACKS_CONFIG));
        
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            assertNotNull(producer);
        }
    }

    @Test
    void testProducerWithRetriesConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        assertEquals(3, props.get(ProducerConfig.RETRIES_CONFIG));
        
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            assertNotNull(producer);
        }
    }

    @Test
    void testKafkaMsgProducerStaticMethodExists() {
        try {
            Class<?> clazz = Class.forName("org.daodao.kafka.KafkaMsgProducer");
            assertNotNull(clazz.getMethod("sendMessage", String.class, String.class));
        } catch (Exception e) {
            fail("KafkaMsgProducer.sendMessage method should exist: " + e.getMessage());
        }
    }

    @Test
    void testProducerConfigValidation() {
        Properties props = new Properties();
        
        // Test required configurations
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        
        // All required configs should be present
        assertTrue(props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertTrue(props.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertTrue(props.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }
}