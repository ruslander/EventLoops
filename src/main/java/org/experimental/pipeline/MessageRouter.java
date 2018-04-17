package org.experimental.pipeline;

import org.experimental.EndpointId;
import org.experimental.MessageBus;
import org.experimental.MessageEnvelope;
import org.experimental.transport.KafkaMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageRouter implements RouteMessagesToHandlers {

    private MessageHandlerTable handlers;
    private KafkaMessageSender sender;
    private EndpointId endpointId;
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageRouter.class);


    public MessageRouter(MessageHandlerTable handlers, KafkaMessageSender sender, EndpointId endpointId) {
        this.handlers = handlers;
        this.sender = sender;
        this.endpointId = endpointId;
    }

    @Override
    public void Route(MessageEnvelope message) {
        try{
            MessageBus messageBus = new MessageBus(sender, message, endpointId);
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
}
