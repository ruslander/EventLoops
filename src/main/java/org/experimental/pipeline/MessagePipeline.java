package org.experimental.pipeline;

import org.experimental.runtime.EndpointId;
import org.experimental.MessageBus;
import org.experimental.MessageEnvelope;
import org.experimental.directions.MessageDestinations;
import org.experimental.transport.KafkaMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagePipeline implements DispatchMessagesToHandlers {

    private MessageHandlerTable handlers;
    private KafkaMessageSender sender;
    private EndpointId endpointId;
    private MessageDestinations router;
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagePipeline.class);

    public MessagePipeline(MessageHandlerTable handlers, KafkaMessageSender sender, EndpointId endpointId, MessageDestinations router) {
        this.handlers = handlers;
        this.sender = sender;
        this.endpointId = endpointId;
        this.router = router;
    }

    @Override
    public void dispatch(MessageEnvelope message) {
        try{
            MessageBus messageBus = netMessageBus(message);
            HandleMessages<Object> handler = this.handlers.getHandlers(messageBus, message.getLocalMessage());

            if(handler == null){
                String simpleName = message.getLocalMessage().getClass().toString();
                LOGGER.warn("No handler registered for {}", simpleName);
                return;
            }

            handler.handle(message.getLocalMessage());
        }catch (Exception e){
            throw e;
        }
    }

    public MessageBus netMessageBus(MessageEnvelope message) {
        return new MessageBus(sender, message, endpointId, router);
    }
}
