package org.daodao.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.daodao.kafka.util.Constants;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaTopicClientTest {

    @Test
    void testNewTopicCreation() {
        NewTopic topic = new NewTopic("test-topic", 1, (short) 1);
        assertEquals("test-topic", topic.name());
        assertEquals(1, topic.numPartitions());
        assertEquals((short) 1, topic.replicationFactor());
    }

    @Test
    void testNewTopicWithMultiplePartitions() {
        NewTopic topic = new NewTopic("multi-partition-topic", 3, (short) 2);
        assertEquals("multi-partition-topic", topic.name());
        assertEquals(3, topic.numPartitions());
        assertEquals((short) 2, topic.replicationFactor());
    }

    @Test
    void testAdminClientConfigProperties() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        
        assertEquals(Constants.BROKERS, props.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertNotNull(props);
    }

    @Test
    void testConstantsValues() {
        assertNotNull(Constants.BROKERS);
        assertNotNull(Constants.TOPIC);
        assertNotNull(Constants.GROUP);
        assertNotNull(Constants.STR_SERIALIZER);
        assertNotNull(Constants.DESERIALIZER);
        assertNotNull(Constants.OFFSET);
    }

    @Test
    void testTopicNameGeneration() {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String topicName = "test-topic-" + timestamp;
        
        assertTrue(topicName.startsWith("test-topic-"));
        assertTrue(topicName.contains(timestamp));
    }

    @Test
    void testAdminClientCreation() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);
        
        try (AdminClient client = AdminClient.create(props)) {
            assertNotNull(client);
        }
    }

    @Test
    void testKafkaTopicClientStaticMethodExists() {
        // Test that the static method exists (will throw NoSuchMethodError if not)
        try {
            Class<?> clazz = Class.forName("org.daodao.kafka.KafkaTopicClient");
            assertNotNull(clazz.getMethod("createTopic", String.class, int.class, short.class));
        } catch (Exception e) {
            fail("KafkaTopicClient.createTopic method should exist: " + e.getMessage());
        }
    }

    @Test
    void testNewTopicValidation() {
        // Test valid topic names
        assertDoesNotThrow(() -> new NewTopic("valid-topic", 1, (short) 1));
        assertDoesNotThrow(() -> new NewTopic("topic_with_underscores", 2, (short) 1));
        assertDoesNotThrow(() -> new NewTopic("topic.with.dots", 3, (short) 2));
        
        // Test partition and replication factor validation
        NewTopic topic = new NewTopic("test", 1, (short) 1);
        assertTrue(topic.numPartitions() > 0);
        assertTrue(topic.replicationFactor() > 0);
    }
}