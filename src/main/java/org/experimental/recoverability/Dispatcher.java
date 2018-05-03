package org.experimental.recoverability;

import org.experimental.MessageEnvelope;
import org.experimental.pipeline.DispatchMessagesToHandlers;
import org.experimental.pipeline.MessagePipeline;
import org.experimental.runtime.EndpointId;
import org.experimental.transport.KafkaMessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public abstract class Dispatcher implements DispatchMessagesToHandlers {

    protected static final Logger LOGGER = LoggerFactory.getLogger(MessagePipeline.class);

    private DispatchMessagesToHandlers messagePipeline;
    protected KafkaMessageSender sender;
    protected String forwardTo;
    private BackOff backoffStep;

    public Dispatcher(DispatchMessagesToHandlers messagePipeline, KafkaMessageSender sender, String forwardTo, BackOff backoffStep) {
        this.messagePipeline = messagePipeline;
        this.sender = sender;
        this.forwardTo = forwardTo;
        this.backoffStep = backoffStep;
    }

    @Override
    public void dispatch(MessageEnvelope message) {
        for (int attempt = 0; attempt < 3; attempt++) {
            try{
                messagePipeline.dispatch(message);
                return;
            }catch (Exception e){

                LOGGER.warn("Attempt#{}", attempt, e);

                long backoffDuration = backoffStep.get(attempt);

                try {
                    Thread.sleep(backoffDuration);
                } catch (InterruptedException e1) {
                }

                if(attempt == 2){
                    escalateTo(message, e);
                    sender.send(Arrays.asList(forwardTo), message);
                }
            }
        }
    }

    abstract void escalateTo(MessageEnvelope message, Exception e);




    public static Dispatcher withSrl(DispatchMessagesToHandlers messagePipeline, KafkaMessageSender sender, EndpointId ep, BackOff flrBackoff){
        return new Dispatcher(messagePipeline, sender, ep.getSlrTopicName(), flrBackoff) {
            @Override
            void escalateTo(MessageEnvelope message, Exception e) {
                LOGGER.info("Escalate message {} to {} topic", message.getUuid(), forwardTo);
            }
        };
    }

    public static Dispatcher withError(DispatchMessagesToHandlers messagePipeline, KafkaMessageSender sender, EndpointId ep, BackOff slrBackoff){
        return new Dispatcher(messagePipeline, sender, ep.getErrorsTopicName(), slrBackoff) {
            @Override
            void escalateTo(MessageEnvelope message, Exception e) {
                LOGGER.error("Escalate message {} to {} topic", message.getUuid(), forwardTo);
            }
        };
    }
}
