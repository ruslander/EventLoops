package org.experimental.pipeline;

import org.experimental.*;
import org.experimental.recoverability.ExponentialBackOff;
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

        for (int attempt = 0; attempt < 3; attempt++) {
            try{
                attemptToProcess(message);
                return;
            }catch (Exception e){

                LOGGER.warn("FLR attempt#{}", attempt, e);

                try {
                    Thread.sleep(ExponentialBackOff.nextTimeout(attempt));
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                if(attempt == 2){
                    scheduleForSlr(message, e);
                }
            }
        }
    }

    public void attemptToProcess(MessageEnvelope message) {
        MessageBus slim = netMessageBus(message);
        UnitOfWork unitOfWork = new UnitOfWork();
        TransactionalMessageBus messageBus = new TransactionalMessageBus(slim, unitOfWork);

        HandleMessages<Object> handler = this.handlers.getHandlers(messageBus, message.getLocalMessage());

        if(handler == null){
            String simpleName = message.getLocalMessage().getClass().toString();
            LOGGER.warn("No handler registered for {}", simpleName);
        }

        handler.handle(message.getLocalMessage());
        unitOfWork.complete();
    }

    private void scheduleForSlr(MessageEnvelope message, Exception e) {
        String slrTopic = endpointId.getSlrTopicName();
        sender.send(Arrays.asList(slrTopic), message);

        LOGGER.info("Schedule message {} for SLR {} topic", message.getUuid(), slrTopic);
    }

    public MessageBus netMessageBus(MessageEnvelope message) {
        return new UnicastMessageBus(sender, message, endpointId, router);
    }
}
