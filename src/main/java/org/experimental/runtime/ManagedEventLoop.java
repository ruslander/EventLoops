package org.experimental.runtime;

import org.apache.kafka.common.errors.InterruptException;
import org.experimental.MessageEnvelope;
import org.experimental.pipeline.DispatchMessagesToHandlers;
import org.experimental.transport.KafkaMessageReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

public class ManagedEventLoop implements Closeable {
    private String name;
    private final String kafka;
    private final List<String> inputTopics;
    private DispatchMessagesToHandlers router;
    private Thread worker;

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedEventLoop.class);

    public ManagedEventLoop(String name, String kafka, List<String> inputTopics, DispatchMessagesToHandlers router) {
        this.name = name;
        this.kafka = kafka;
        this.inputTopics = inputTopics;
        this.router = router;
    }

    public void start(){
        this.worker = new Thread(this::StartReceiving);
        this.worker.start();

        LOGGER.info("started loop " + name);
    }

    @Override
    public void close(){
        worker.interrupt();

        LOGGER.info("stopped loop for " + name);
    }

    public void StartReceiving() {
        KafkaMessageReceiver receiver = new KafkaMessageReceiver(kafka, inputTopics);
        receiver.start();

        while (true){
            try {
                List<MessageEnvelope> messages = receiver.receive();

                //LOGGER.debug("Poll {}", messages.size());

                for (MessageEnvelope env: messages) {
                    LOGGER.debug("Dispatch to router {}", router.getClass().getSimpleName());
                    router.dispatch(env);
                    LOGGER.debug("Dispatch completed");
                }
            } catch (InterruptException e) {
                break;
            } catch (Exception e) {
                LOGGER.warn("Message processing failed", e);
            }
        }
    }
}
