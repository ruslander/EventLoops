package org.experimental.pipeline;

import org.experimental.*;
import org.experimental.runtime.EndpointId;
import org.experimental.directions.MessageDestinations;
import org.experimental.transport.KafkaMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


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
            MessageBus slim = netMessageBus(message);
            UnitOfWork unitOfWork = new UnitOfWork();
            TransactionalMessageBus messageBus = new TransactionalMessageBus(slim, unitOfWork);

            HandleMessages<Object> handler = this.handlers.getHandlers(messageBus, message.getLocalMessage());

            if(handler == null){
                String simpleName = message.getLocalMessage().getClass().toString();
                LOGGER.warn("No handler registered for {}", simpleName);
                return;
            }

            handler.handle(message.getLocalMessage());
            unitOfWork.complete();
        }catch (Exception e){
            handleFailure(message, e);
            throw e;
        }
    }

    private void handleFailure(MessageEnvelope message, Exception e) {
        String errorsTopic = endpointId.getErrorsTopicName();
        sender.send(Arrays.asList(errorsTopic), message);

        LOGGER.info("Forwarded message {} to the error {} topic", message.getUuid(), errorsTopic);
    }

    public MessageBus netMessageBus(MessageEnvelope message) {
        return new UnicastMessageBus(sender, message, endpointId, router);
    }
}
