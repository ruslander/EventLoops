package org.experimental.runtime;

import org.experimental.MessageBus;
import org.experimental.directions.MessageDestinations;
import org.experimental.directions.MessageSubscriptions;
import org.experimental.pipeline.HandleMessages;
import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.pipeline.MessagePipeline;
import org.experimental.transport.KafkaMessageSender;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class EndpointWire implements Closeable{
    private final EndpointId endpointId;
    private final String kafkaConnection;
    private final KafkaMessageSender sender;
    private final MessageHandlerTable table = new MessageHandlerTable();
    private final MessageDestinations router = new MessageDestinations();
    private final MessageSubscriptions subscriptions = new MessageSubscriptions();
    private final MessagePipeline pipeline;
    private ManagedEventLoop inputEventLoop;
    private ManagedEventLoop subscriptionsEventLoop;
    private final List<String> inputTopics;

    public EndpointWire(String endpoint,String kafkaConnection) {
        this.endpointId = new EndpointId(endpoint);
        this.kafkaConnection = kafkaConnection;
        this.sender = new KafkaMessageSender(kafkaConnection);
        this.pipeline = new MessagePipeline(table, sender, endpointId, router);
        this.inputTopics = Arrays.asList(endpointId.getInputTopicName());

        subscriptionsEventLoop = newLoop("subs." + endpointId.getInputTopicName(), subscriptions.sources());
        inputEventLoop = newLoop("main."+ endpointId.getInputTopicName(), inputTopics);
    }

    public void configure(){

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

    public void registerEndpoint(String endpointId, Class<?> ... types) {
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
}
