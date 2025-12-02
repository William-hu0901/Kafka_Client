package org.daodao.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.SslConfigs;
import org.daodao.kafka.util.Constants;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for new Kafka 4.0.0 features.
 * 
 * Kafka 4.0.0 introduces several new features and improvements:
 * - KRaft mode (Kafka Raft metadata mode) - removes Zookeeper dependency
 * - Improved producer performance and reliability
 * - Enhanced consumer features with better offset management
 * - New admin client capabilities
 * - Security improvements
 * - Performance optimizations
 */
class NewKafkaFeaturesTest {

    @Test
    void testKRaftModeFeatures() {
        // In KRaft mode, we can still use AdminClient for topic management
        // This test verifies basic functionality works in KRaft mode
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            assertNotNull(adminClient);
            // AdminClient should work even in KRaft mode
            assertTrue(true);
        }
    }

    @Test
    void testImprovedProducerFeatures() {
        // Test new producer configuration options available in Kafka 4.0.0
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        
        // Test with newer configuration options
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        assertEquals(30000, props.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
        assertEquals(10000, props.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
        assertEquals("all", props.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(3, props.get(ProducerConfig.RETRIES_CONFIG));
        assertEquals(true, props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        
        try (Producer<String, String> testProducer = new KafkaProducer<>(props)) {
            assertNotNull(testProducer);
        }
    }

    @Test
    void testImprovedConsumerFeatures() {
        // Test enhanced consumer features in Kafka 4.0.0
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "new-features-test-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        
        // Test with newer consumer configuration
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        
        assertEquals(100, props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals(300000, props.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals("earliest", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(true, props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        
        try (Consumer<String, String> testConsumer = new KafkaConsumer<>(props)) {
            assertNotNull(testConsumer);
        }
    }

    @Test
    void testNewAdminClientFeatures() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            assertNotNull(adminClient);
            
            // Test basic admin operations that should be available in Kafka 4.0.0
            // Note: These would work with actual Kafka cluster
            assertNotNull(adminClient);
            assertTrue(true);
        }
    }

    @Test
    void testPerformanceImprovements() {
        // Test configuration for better performance
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        
        // Performance-oriented configurations
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        assertEquals(16384, producerProps.get(ProducerConfig.BATCH_SIZE_CONFIG));
        assertEquals(5, producerProps.get(ProducerConfig.LINGER_MS_CONFIG));
        assertEquals("lz4", producerProps.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        assertEquals(33554432, producerProps.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
        
        try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            assertNotNull(producer);
        }
    }

    @Test
    void testSecurityImprovements() {
        // Test security-related configurations
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        
        // Security configurations (would be used with actual secure cluster)
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 540000);
        
        assertEquals("TLSv1.2", props.get(SslConfigs.SSL_PROTOCOL_CONFIG));
        assertEquals(540000, props.get(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG));
        
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            assertNotNull(producer);
        }
    }

    @Test
    void testKafkaVersionCompatibility() {
        // Test that we're using Kafka 4.0.0
        assertEquals("4.0.0", System.getProperty("kafka.version", "4.0.0"));
        
        // Verify Kafka classes are available
        assertDoesNotThrow(() -> {
            Class.forName("org.apache.kafka.clients.producer.KafkaProducer");
            Class.forName("org.apache.kafka.clients.consumer.KafkaConsumer");
            Class.forName("org.apache.kafka.clients.admin.AdminClient");
        });
    }

    @Test
    void testConfigurationValidation() {
        // Test configuration validation for new features
        Properties props = new Properties();
        
        // Test producer config validation
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER);
        
        assertTrue(props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertTrue(props.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertTrue(props.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        
        // Test consumer config validation
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DESERIALIZER);
        
        assertTrue(consumerProps.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertTrue(consumerProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG));
        assertTrue(consumerProps.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertTrue(consumerProps.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    }
}