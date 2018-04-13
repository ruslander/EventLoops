package org.experimental.pipeline;

import org.experimental.MessageEnvelope;

import java.util.List;

public class MessageRouter implements RouteMessagesToHandlers {

    private MessageHandlerTable handlers;

    public MessageRouter(MessageHandlerTable handlers) {
        this.handlers = handlers;
    }

    @Override
    public void Route(MessageEnvelope message) {
        try{
            List<HandleMessages<Object>> handlers = this.handlers.getHandlers(message.getLocalMessage());

            for (HandleMessages<Object> handler: handlers){
                handler.handle(message.getLocalMessage());
            }
        }catch (Exception e){
            throw e;
        }
    }
}
