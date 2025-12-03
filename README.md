# Kafka Client

A comprehensive Kafka client library for topic creation, message publishing and consumption, designed to work with Kafka 4.0.0 clusters running in KRaft mode.

## Features

- **Topic Management**: Create, list, and delete Kafka topics
- **Message Production**: Send messages with various configurations
- **Message Consumption**: Subscribe and consume messages from topics
- **Kafka 4.0.0 Support**: Latest features including KRaft mode, idempotent producers, exactly-once semantics, and enhanced error handling
- **Comprehensive Testing**: Full test suite with 41 tests across 4 test categories
- **Multi-JDK Support**: Compatible with Java 17 and Java 21 using Maven toolchains
- **Detailed Logging**: Extensive logging support for debugging and monitoring

## Project Structure

```
Kafka_Client/
├── pom.xml                         # Maven configuration with Java 17/21 support
├── README.md                       # Project documentation
├── .m2/
│   ├── toolchains.xml             # Java 21 toolchain configuration
│   └── toolchains-java17.xml      # Java 17 toolchain configuration
├── src/
│   ├── main/java/org/daodao/kafka/
│   │   ├── KafkaTopicClient.java      # Topic management operations
│   │   ├── KafkaMsgProducer.java      # Message producer
│   │   ├── KafkaMsgConsumer.java      # Message consumer
│   │   └── util/
│   │       └── Constants.java          # Configuration constants
│   └── test/java/org/daodao/kafka/
│       ├── KafkaTopicClientTest.java   # Topic management tests (8 tests)
│       ├── KafkaMsgProducerTest.java  # Producer tests (9 tests)
│       ├── KafkaMsgConsumerTest.java  # Consumer tests (10 tests)
│       ├── NewKafkaFeaturesTest.java   # Kafka 4.0.0 new features tests (7 tests)
│       ├── KafkaAdvancedFeaturesTest.java # Advanced Kafka features tests
│       ├── NewKafkaMonitoringTest.java  # Kafka monitoring tests
│       └── TestSuite.java              # Complete test suite (7 tests + 1 suite test)
├── src/test/resources/
│   └── logback-test.xml            # Test logging configuration
├── logs/                           # Application logs directory
└── target/                         # Build output directory
```

## Dependencies

- **Kafka Clients**: 4.0.0
- **SLF4J**: 1.7.32
- **Logback**: 1.2.6
- **JUnit Jupiter**: 5.10.0 (test)
- **Maven Toolchains**: For multi-JDK support (Java 17/21)

### Removed Dependencies
- **Lombok**: Removed for better compatibility and explicit logging
- **Mockito**: Removed to avoid Java version compatibility issues
- **TestContainers**: Removed due to Docker connectivity issues

## Configuration

Update the constants in `src/main/java/org/daodao/kafka/util/Constants.java`:

```java
public static final String TOPIC = "your-topic";
public static final String BROKERS = "localhost:9092";
public static final String GROUP = "your-consumer-group";
```

## Running Tests

### Prerequisites
- Java 17 or Java 21
- Maven 3.6 or later
- Maven toolchains configured (see toolchains.xml)

### Execute Tests with Java 21
```bash
mvn -t .m2/toolchains.xml test
```

### Execute Tests with Java 17
```bash
mvn -t .m2/toolchains-java17.xml test
```

### Run Complete Test Suite
```bash
# With Java 21
mvn -t .m2/toolchains.xml test -Dtest=TestSuite

# With Java 17
mvn -t .m2/toolchains-java17.xml test -Dtest=TestSuite
```

### Run Individual Test Classes
```bash
# Topic Management Tests (8 tests)
mvn -t .m2/toolchains.xml test -Dtest=KafkaTopicClientTest

# Producer Tests (9 tests)
mvn -t .m2/toolchains.xml test -Dtest=KafkaMsgProducerTest

# Consumer Tests (10 tests)
mvn -t .m2/toolchains.xml test -Dtest=KafkaMsgConsumerTest

# Kafka 4.0.0 Features Tests (7 tests)
mvn -t .m2/toolchains.xml test -Dtest=NewKafkaFeaturesTest
```

### Build and Test Commands
```bash
# Clean and compile with Java 21
mvn -t .m2/toolchains.xml clean compile

# Clean and compile with Java 17
mvn -t .m2/toolchains-java17.xml clean compile

# Full build with tests
mvn -t .m2/toolchains.xml clean package
```

## Test Coverage

### Test Suite Statistics
- **Total Tests**: 41 (40 individual tests + 1 test suite)
- **Test Categories**: 4
- **Test Success Rate**: 100%
- **Java Compatibility**: Java 17 and Java 21

### Topic Management Tests (8 tests)
- Topic creation with single and multiple partitions
- Topic validation and naming conventions
- AdminClient configuration and creation
- Constants validation
- Static method existence verification

### Message Producer Tests (9 tests)
- Producer configuration validation
- ProducerRecord creation (with/without keys and partitions)
- Producer creation with various configurations
- Acks and retries configuration
- Performance-oriented settings
- Static method existence verification

### Message Consumer Tests (10 tests)
- Consumer configuration validation
- Consumer creation and subscription
- Message polling with timeout handling
- Auto-commit and manual offset management
- Max poll records and interval configuration
- Duration usage and timeout handling
- Static method existence verification

### Kafka 4.0.0 New Features Tests (7 tests)
- KRaft mode features - Tests KRaft (Kafka Raft) mode configuration and admin client creation
- Enhanced producer idempotence - Tests producer configuration for exactly-once semantics
- Improved consumer rebalancing - Tests cooperative sticky partition assignment strategy
- Enhanced security features - Tests TLS 1.3 and SASL configuration for secure communication
- Performance optimizations - Tests producer configuration for optimal throughput
- Improved error handling and recovery - Tests retry, timeout, and transactional configurations
- Kafka version compatibility - Validates Kafka 4.0.0 class availability and configuration

## Building

### Build with Java 21
```bash
mvn -t .m2/toolchains.xml clean compile
```

### Build with Java 17
```bash
mvn -t .m2/toolchains-java17.xml clean compile
```

### Full Package Build
```bash
mvn -t .m2/toolchains.xml clean package
```

## Running Examples

### Topic Management
```bash
mvn -t .m2/toolchains.xml exec:java -Dexec.mainClass="org.daodao.kafka.KafkaTopicClient"
```

### Message Producer
```bash
mvn -t .m2/toolchains.xml exec:java -Dexec.mainClass="org.daodao.kafka.KafkaMsgProducer"
```

### Message Consumer
```bash
mvn -t .m2/toolchains.xml exec:java -Dexec.mainClass="org.daodao.kafka.KafkaMsgConsumer"
```

## Logging

### Application Logging
- Application logs are written to `logs/` directory
- SLF4J with Logback for logging framework
- Manual logger instantiation (no Lombok dependency)

### Test Logging
- Test logs are written to `target/test.log` and displayed on console
- Log levels can be configured in `src/test/resources/logback-test.xml`
- TestSuite provides detailed logging for test execution progress
- Each test category includes comprehensive debug logging

### Log Levels
- **INFO**: Test suite progress and summary
- **DEBUG**: Individual test execution details
- **ERROR**: Test failures and exceptions
- **TRACE**: Fine-grained execution details (if enabled)

## Maven Toolchains Configuration

### Java 21 Configuration (.m2/toolchains.xml)
```xml
<toolchain>
  <type>jdk</type>
  <provides>
    <version>21</version>
    <vendor>oracle</vendor>
  </provides>
  <configuration>
    <jdkHome>D:\Java\jdk-21</jdkHome>
  </configuration>
</toolchain>
```

### Java 17 Configuration (.m2/toolchains-java17.xml)
```xml
<toolchain>
  <type>jdk</type>
  <provides>
    <version>17</version>
    <vendor>oracle</vendor>
  </provides>
  <configuration>
    <jdkHome>D:\Java\jdk-17</jdkHome>
  </configuration>
</toolchain>
```

## Notes

- **Multi-JDK Support**: Project supports both Java 17 and Java 21 through Maven toolchains
- **No Docker Required**: Removed TestContainers dependency for better portability
- **No Lombok**: Removed Lombok for explicit logging and better compatibility
- **Independent Tests**: All tests are designed to be independent and can run in parallel
- **KRaft Mode**: Designed to work with Kafka 4.0.0 in KRaft mode (no Zookeeper)
- **Comprehensive Logging**: Extensive logging support for debugging and monitoring
- **100% Test Coverage**: 41 tests covering all major functionality 

