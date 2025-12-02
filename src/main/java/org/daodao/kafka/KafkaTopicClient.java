package org.daodao.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.daodao.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaTopicClient {
    private static final Logger log = LoggerFactory.getLogger(KafkaTopicClient.class);
    public static void main(String[] args) {
        createTopic(Constants.TOPIC, 1, (short) 1);
    }

    public static void createTopic(String topicName, int partitions, short replicationFactor) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            log.info("Topic created successfully, topic: {}", topicName);
            adminClient.deleteTopics(Collections.singleton(topicName)).all().get();
            log.info("Topic deleted successfully, topic: {}", topicName);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Create topic failed, topic: {}", topicName, e);
        }
    }


}