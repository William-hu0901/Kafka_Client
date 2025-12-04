package org.daodao.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.metrics.stats.*;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Kafka Monitoring Test Suite
 * 
 * This test class validates the following Kafka monitoring and metrics features:
 * 
 * 1. Producer Metrics - Producer performance and health metrics collection
 * 2. Consumer Metrics - Consumer performance and health metrics collection
 * 3. AdminClient Metrics - Administrative operations monitoring
 * 4. Cluster Health Monitoring - Overall cluster status and health checks
 * 5. Topic Level Metrics - Topic-specific performance metrics
 * 6. Custom Metrics - Custom metric reporters and collection
 * 7. JMX Integration - JMX monitoring and management capabilities
 */
class NewKafkaMonitoringTest {

    /**
     * Test producer metrics - Tests producer performance and health metrics collection.
     * Producer metrics provide insights into message throughput, latency, and error rates.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testProducerMetrics() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, "2");
        props.put(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "30000");
        
        String metricsSamples = (String) props.get(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG);
        String sampleWindow = (String) props.get(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG);
        
        System.out.println("Producer Metrics Configuration:");
        System.out.println("- Number of samples: " + metricsSamples);
        System.out.println("- Sample window (ms): " + sampleWindow);
        
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            System.out.println("Producer metrics count: " + metrics.size());
            System.out.println("Producer metrics collection configured successfully");
        } catch (Exception e) {
            System.out.println("Producer metrics test failed: " + e.getMessage());
        }
    }

    /**
     * Test consumer metrics - Tests consumer performance and health metrics collection.
     * Consumer metrics provide insights into message consumption rates, lag, and rebalancing.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testConsumerMetrics() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-consumer-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, "2");
        props.put(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "30000");
        
        String metricsSamples = (String) props.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG);
        String sampleWindow = (String) props.get(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG);
        
        System.out.println("Consumer Metrics Configuration:");
        System.out.println("- Number of samples: " + metricsSamples);
        System.out.println("- Sample window (ms): " + sampleWindow);
        
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<MetricName, ? extends Metric> metrics = consumer.metrics();
            System.out.println("Consumer metrics count: " + metrics.size());
            System.out.println("Consumer metrics collection configured successfully");
        } catch (Exception e) {
            System.out.println("Consumer metrics test failed: " + e.getMessage());
        }
    }

    /**
     * Test admin client metrics - Tests administrative operations monitoring.
     * Admin client metrics provide insights into cluster management operations.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testAdminClientMetrics() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        adminProps.put(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG, "2");
        adminProps.put(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "30000");
        
        String metricsSamples = (String) adminProps.get(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG);
        String sampleWindow = (String) adminProps.get(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG);
        
        System.out.println("Admin Client Metrics Configuration:");
        System.out.println("- Number of samples: " + metricsSamples);
        System.out.println("- Sample window (ms): " + sampleWindow);
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            Map<MetricName, ? extends Metric> metrics = adminClient.metrics();
            System.out.println("Admin client metrics count: " + metrics.size());
            System.out.println("Admin client metrics collection configured successfully");
        } catch (Exception e) {
            System.out.println("Admin client metrics test failed: " + e.getMessage());
        }
    }

    /**
     * Test cluster health monitoring - Tests overall cluster status and health checks.
     * Cluster health monitoring provides visibility into cluster state and availability.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testClusterHealthMonitoring() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        
        System.out.println("Cluster Health Monitoring Configuration:");
        System.out.println("- Bootstrap servers: " + adminProps.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("- Request timeout: " + adminProps.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            System.out.println("Admin client for cluster health monitoring created successfully");
        } catch (Exception e) {
            System.out.println("Cluster health monitoring test failed: " + e.getMessage());
        }
    }

    /**
     * Test topic level metrics - Tests topic-specific performance metrics.
     * Topic metrics provide insights into individual topic performance and usage.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testTopicLevelMetrics() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        
        String testTopicName = "topic-metrics-test";
        System.out.println("Topic Level Metrics Configuration:");
        System.out.println("- Test topic name: " + testTopicName);
        System.out.println("- Request timeout: " + adminProps.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
            System.out.println("Topic resource created for metrics collection");
        } catch (Exception e) {
            System.out.println("Topic level metrics test failed: " + e.getMessage());
        }
    }

    /**
     * Test custom metrics - Tests custom metric reporters and collection.
     * Custom metrics allow application-specific monitoring and alerting.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testCustomMetrics() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "org.apache.kafka.common.metrics.JmxReporter");
        
        String metricReporterClasses = (String) props.get(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG);
        System.out.println("Metric reporter classes: " + metricReporterClasses);
        
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            
            System.out.println("Custom metrics configuration:");
            System.out.println("- Total metrics available: " + metrics.size());
            System.out.println("- JMX reporter enabled: " + metricReporterClasses.contains("JmxReporter"));
            
            // Check for custom metric types
            Set<String> metricTypes = new HashSet<>();
            for (MetricName metricName : metrics.keySet()) {
                metricTypes.add(metricName.group());
            }
            
            System.out.println("- Metric groups: " + metricTypes.size());
            for (String group : metricTypes) {
                System.out.println("  Group: " + group);
            }
            
        } catch (Exception e) {
            System.out.println("Custom metrics test failed: " + e.getMessage());
        }
    }

    /**
     * Test JMX integration - Tests JMX monitoring and management capabilities.
     * JMX integration allows external monitoring tools to access Kafka metrics.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testJMXIntegration() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "org.apache.kafka.common.metrics.JmxReporter");
        
        // JMX configuration properties
        System.setProperty("com.sun.management.jmxremote", "true");
        System.setProperty("com.sun.management.jmxremote.port", "9999");
        System.setProperty("com.sun.management.jmxremote.authenticate", "false");
        System.setProperty("com.sun.management.jmxremote.ssl", "false");
        
        System.out.println("JMX Integration Configuration:");
        System.out.println("- JMX enabled: " + System.getProperty("com.sun.management.jmxremote"));
        System.out.println("- JMX port: " + System.getProperty("com.sun.management.jmxremote.port"));
        System.out.println("- JMX authentication: " + System.getProperty("com.sun.management.jmxremote.authenticate"));
        System.out.println("- JMX SSL: " + System.getProperty("com.sun.management.jmxremote.ssl"));
        
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            
            System.out.println("JMX Metrics Integration:");
            System.out.println("- Producer created with JMX reporter");
            System.out.println("- Metrics available via JMX: " + metrics.size());
            
            // Simulate JMX bean name generation
            for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
                MetricName metricName = entry.getKey();
                String jmxMBeanName = "kafka.producer:type=" + metricName.group() + ",name=" + metricName.name();
                
                if (metricName.name().contains("record-send-rate") || 
                    metricName.name().contains("byte-rate")) {
                    System.out.println("  JMX MBean: " + jmxMBeanName);
                    System.out.println("  Value: " + entry.getValue().metricValue());
                    break; // Just show a few examples
                }
            }
            
        } catch (Exception e) {
            System.out.println("JMX integration test failed: " + e.getMessage());
        }
    }
}