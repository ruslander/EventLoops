package org.experimental;

import org.experimental.directions.MessageDestinations;
import org.experimental.runtime.EndpointId;
import org.experimental.transport.KafkaMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class MessageBus {

    private KafkaMessageSender transport;
    private MessageEnvelope envelope;
    private EndpointId endpointId;
    private MessageBuilder builder;
    private MessageDestinations router;

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBus.class);

    public MessageBus(KafkaMessageSender transport, MessageEnvelope envelope, EndpointId endpointId, MessageDestinations router) {
        this.transport = transport;
        this.envelope = envelope;
        this.endpointId = endpointId;
        this.builder = new MessageBuilder(endpointId.getInputTopicName());
        this.router = router;
    }

    public void publish(Object message) {
        List<String> dest = Arrays.asList(endpointId.getEventsTopicName());
        MessageEnvelope envelope = builder.buildMessage(message);
        transport.send(dest, envelope);

        LOGGER.info("Published {}", message.getClass().getSimpleName());
    }

    public void send(Object message) {
        List<String> dest = router.destinations(message.getClass());
        MessageEnvelope envelope = builder.buildMessage(message);
        transport.send(dest, envelope);

        LOGGER.info("Sent {} ", message.getClass().getSimpleName());
    }

    public void reply(Object message) {
        List<String> dest = Arrays.asList(envelope.getReturnAddress());
        MessageEnvelope envelope = builder.buildMessage(message);
        transport.send(dest, envelope);

        LOGGER.info("Replied with {}", message.getClass().getSimpleName());
    }
}
