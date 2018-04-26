package org.experimental.runtime;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.experimental.MessageBus;
import org.experimental.diagnostics.ConfigurationInspector;
import org.experimental.directions.MessageDestinations;
import org.experimental.directions.MessageSubscriptions;
import org.experimental.pipeline.HandleMessages;
import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.pipeline.MessagePipeline;
import org.experimental.transport.KafkaMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class EndpointWire implements Closeable{
    private final EndpointId endpointId;
    private final String kafkaConnection;
    private final KafkaMessageSender sender;
    private final String zookeeper;
    private final MessageHandlerTable table = new MessageHandlerTable();
    private final MessageDestinations router = new MessageDestinations();
    private final MessageSubscriptions subscriptions = new MessageSubscriptions();
    private final MessagePipeline pipeline;
    private ManagedEventLoop inputEventLoop;
    private ManagedEventLoop subscriptionsEventLoop;
    private final List<String> inputTopics;

    private final ConfigurationInspector inspector;

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedEventLoop.class);

    public EndpointWire(String endpoint,String kafkaConnection, String zookeeper) {
        this.endpointId = new EndpointId(endpoint);
        this.kafkaConnection = kafkaConnection;
        this.sender = new KafkaMessageSender(kafkaConnection);
        this.zookeeper = zookeeper;
        this.pipeline = new MessagePipeline(table, sender, endpointId, router);
        this.inputTopics = Arrays.asList(endpointId.getInputTopicName());
        this.inspector = new ConfigurationInspector(endpointId);
    }

    public void configure(){

        inspector.inspectHandlers(table);
        inspector.inspectSubscriptions(subscriptions);
        inspector.inspectRouting(router);
        inspector.present();

        createTopic(endpointId.getInputTopicName(), 1, 1, new Properties());
        createTopic(endpointId.getEventsTopicName(), 1, 1, new Properties());
        createTopic(endpointId.getErrorsTopicName(), 1, 1, new Properties());

        subscriptionsEventLoop = newLoop(endpointId.getInputTopicName() + "-rx", subscriptions.sources());
        inputEventLoop = newLoop(endpointId.getInputTopicName()+ "-active", inputTopics);

        if(!subscriptions.sources().isEmpty())
            subscriptionsEventLoop.start();

        inputEventLoop.start();
        sender.start();
    }

    @Override
    public void close() throws IOException {
        if(!subscriptions.sources().isEmpty())
            subscriptionsEventLoop.close();

        inputEventLoop.close();
        sender.stop();
    }

    private ManagedEventLoop newLoop(String name, List<String> topics){
        return new ManagedEventLoop(name, kafkaConnection, topics, pipeline);
    }

    public void registerEndpointRoute(String endpointId, Class<?> ... types) {
        router.registerEndpoint(endpointId, types);
    }

    public void subscribeToEndpoint(String endpointId, Class<?> ... types) {
        subscriptions.subscribeToEndpoint(endpointId, types);
    }

    public <T> void registerHandler(Class<T> c, Function<MessageBus, HandleMessages<T>> handler) {
        table.registerHandler(c, handler);
    }

    public MessageBus getMessageBus() {
        return pipeline.netMessageBus(null);
    }

    private static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 10 * 1000;
    private static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000;

    public void createTopic(String topic,
                            int partitions,
                            int replication,
                            Properties topicConfig) {
        LOGGER.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
                topic, partitions, replication, topicConfig);
        ZkClient zkClient = new ZkClient(
                zookeeper,
                DEFAULT_ZK_SESSION_TIMEOUT_MS,
                DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                ZKStringSerializer$.MODULE$);
        boolean isSecure = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeper), isSecure);
        AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
        zkClient.close();
    }
}
