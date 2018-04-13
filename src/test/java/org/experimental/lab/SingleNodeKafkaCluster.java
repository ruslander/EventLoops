package org.experimental.lab;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Console;
import scala.collection.Map;
import scala.collection.Set;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class SingleNodeKafkaCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeKafkaCluster.class);

    private final String zookeeperString;
    private final String brokerString;
    private final int zkPort;
    private final int brokerPort;
    private final Properties kafkaBrokerConfig = new Properties();

    private static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 10 * 1000;
    private static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000;

    private KafkaServerStartable broker;
    private ServerCnxnFactory zookeeper;
    private KafkaProducer<String, String> producer;
    private File logDir;

    public String getZookeeperString() {
        return zookeeperString;
    }

    public String getKfkConnectionString() {
        return brokerString;
    }

    public SingleNodeKafkaCluster() throws IOException {
        this(getEphemeralPort(), getEphemeralPort());
    }

    public SingleNodeKafkaCluster(String zkConnectionString, String kafkaConnectionString) {
        this(parseConnectionString(zkConnectionString), parseConnectionString(kafkaConnectionString));
    }

    public SingleNodeKafkaCluster(int zkPort, int brokerPort) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.zookeeperString = "localhost:" + zkPort;
        this.brokerString = "localhost:" + brokerPort;
        this.producer = createProducer();
    }

    private KafkaProducer<String, String> createProducer() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", brokerString);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static int parseConnectionString(String connectionString) {
        try {
            String[] hostPorts = connectionString.split(",");

            if (hostPorts.length != 1) {
                throw new IllegalArgumentException("Only one 'host:port' pair is allowed in connection string");
            }

            String[] hostPort = hostPorts[0].split(":");

            if (hostPort.length != 2) {
                throw new IllegalArgumentException("Invalid format of a 'host:port' pair");
            }

            if (!"localhost".equals(hostPort[0])) {
                throw new IllegalArgumentException("Only localhost is allowed for KafkaUnit");
            }

            return Integer.parseInt(hostPort[1]);
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse connectionString " + connectionString, e);
        }
    }

    private static int getEphemeralPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public void startup() {
        zookeeper = wireZookeeper(zkPort);

        try {
            logDir = Files.createTempDirectory("kafka").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }
        logDir.deleteOnExit();
        Runtime.getRuntime().addShutdownHook(new Thread(getDeleteLogDirectoryAction()));
        kafkaBrokerConfig.setProperty("zookeeper.connect", zookeeperString);
        kafkaBrokerConfig.setProperty("broker.id", "1");
        kafkaBrokerConfig.setProperty("host.name", "localhost");
        kafkaBrokerConfig.setProperty("port", Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));
        kafkaBrokerConfig.setProperty("delete.topic.enable", String.valueOf(true));
        kafkaBrokerConfig.setProperty("auto.create.topics.enable", String.valueOf(true));
        kafkaBrokerConfig.setProperty("offsets.topic.replication.factor", "1");

        KafkaConfig config = new KafkaConfig(kafkaBrokerConfig);

        /*Map<String, ?> treeMap = new TreeMap<>(config.values());
        for (Map.Entry<?, ?> entry : treeMap.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }*/

        broker = new KafkaServerStartable(config);
        broker.startup();

        LOGGER.info("Single node Kafka cluster is ready");
    }


    public ServerCnxnFactory wireZookeeper(int port) {

        final File snapshotDir;
        final File logDir;
        try {
            snapshotDir = java.nio.file.Files.createTempDirectory("zookeeper-snapshot").toFile();
            logDir = java.nio.file.Files.createTempDirectory("zookeeper-logs").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    FileUtils.deleteDirectory(snapshotDir);
                    FileUtils.deleteDirectory(logDir);
                }
                catch(IOException e) {
                    // We tried!
                }
            }

        });

        ServerCnxnFactory factory = null;

        try {
            int tickTime = 500;
            ZooKeeperServer zkServer = new ZooKeeperServer(snapshotDir, logDir, tickTime);
            factory = NIOServerCnxnFactory.createFactory();
            factory.configure(new InetSocketAddress("localhost", port), 16);
            factory.startup(zkServer);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start ZooKeeper", e);
        }

        return factory;
    }

    private Runnable getDeleteLogDirectoryAction() {
        return () -> {
            if (logDir != null) {
                try {
                    FileUtils.deleteDirectory(logDir);
                } catch (IOException e) {
                    LOGGER.warn("Problems deleting temporary directory " + logDir.getAbsolutePath(), e);
                }
            }
        };
    }

    public String getKafkaConnect() {
        return brokerString;
    }

    public int getZkPort() {
        return zkPort;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void createTopic(String topic) {
        createTopic(topic, 1, 1, new Properties());
    }

    public void createTopic(String topic,
                            int partitions,
                            int replication,
                            Properties topicConfig) {
        LOGGER.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
                topic, partitions, replication, topicConfig);
        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
        // topic.
        ZkClient zkClient = new ZkClient(
                zookeeperString,
                DEFAULT_ZK_SESSION_TIMEOUT_MS,
                DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                ZKStringSerializer$.MODULE$);
        boolean isSecure = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperString), isSecure);
        AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
        zkClient.close();
    }

    public void deleteTopic(String topic) {
        LOGGER.debug("Deleting topic {}", topic);
        ZkClient zkClient = new ZkClient(
                zookeeperString,
                DEFAULT_ZK_SESSION_TIMEOUT_MS,
                DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                ZKStringSerializer$.MODULE$);
        boolean isSecure = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperString), isSecure);
        AdminUtils.deleteTopic(zkUtils, topic);
        zkClient.close();
    }

    public Set<String> listTopics() {
        LOGGER.debug("List topics");
        ZkClient zkClient = new ZkClient(
                zookeeperString,
                DEFAULT_ZK_SESSION_TIMEOUT_MS,
                DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                ZKStringSerializer$.MODULE$);
        boolean isSecure = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperString), isSecure);
        Map<String, Properties> topicConfigs = AdminUtils.fetchAllTopicConfigs(zkUtils);
        zkClient.close();

        return topicConfigs.keySet();
    }

    public void shutdown() {
        if (broker != null) {
            broker.shutdown();
            broker.awaitShutdown();
        }
        if (zookeeper != null)
            zookeeper.shutdown();
    }

    public List<ConsumerRecord<String, String>> readRecords(final String topicName, final int maxPoll) {
        return readMessages(topicName, maxPoll, new PasstroughMessageExtractor());
    }

    public List<String> readMessages(final String topicName, final int maxPoll) {
        return readMessages(topicName, maxPoll, new ValueMessageExtractor());
    }

    public List<String> readAllMessages(final String topicName) {
        return readMessages(topicName, Integer.MAX_VALUE, new ValueMessageExtractor());
    }

    private <T> List<T> readMessages(final String topicName, final int maxPoll, final MessageExtractor<T> messageExtractor) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", brokerString);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("max.poll.records", String.valueOf(maxPoll));
        try (final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props)) {
            kafkaConsumer.subscribe(Collections.singletonList(topicName));
            kafkaConsumer.poll(0); // dummy poll
            kafkaConsumer.seekToBeginning(Collections.singletonList(new TopicPartition(topicName, 0)));
            final ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            final List<T> messages = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                messages.add(messageExtractor.extract(record));
            }
            return messages;
        }
    }


    @SafeVarargs
    public final void sendMessages(final ProducerRecord<String, String>... records) {
        for (final ProducerRecord<String, String> record: records) {
            try {
                producer.send(record).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            } finally {
                producer.flush();
            }
        }
    }

    /**
     * Set custom broker configuration.
     * See available config keys in the kafka documentation: http://kafka.apache.org/documentation.html#brokerconfigs
     */
    public final void setKafkaBrokerConfig(String configKey, String configValue) {
        kafkaBrokerConfig.setProperty(configKey, configValue);
    }

    private interface MessageExtractor<T> {
        T extract(ConsumerRecord<String, String> record);
    }

    public class ValueMessageExtractor implements MessageExtractor<String> {
        @Override
        public String extract(final ConsumerRecord<String, String> record) {
            return record.value();
        }
    }

    public class PasstroughMessageExtractor implements MessageExtractor<ConsumerRecord<String, String>> {
        @Override
        public ConsumerRecord<String, String> extract(final ConsumerRecord<String, String> record)
        {
            return record;
        }
    }
}
