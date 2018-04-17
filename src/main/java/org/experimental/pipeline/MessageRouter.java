package org.experimental.pipeline;

import org.experimental.MessageBus;
import org.experimental.MessageEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageRouter implements RouteMessagesToHandlers {

    private MessageHandlerTable handlers;
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageRouter.class);


    public MessageRouter(MessageHandlerTable handlers) {
        this.handlers = handlers;
    }

    @Override
    public void Route(MessageEnvelope message) {
        try{
            HandleMessages<Object> handler = this.handlers.getHandlers(new MessageBus(), message.getLocalMessage());

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
