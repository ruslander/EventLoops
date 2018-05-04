package org.experimental.recoverability;

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
        if(inputTopics.isEmpty())
            throw new RuntimeException("Subscription with 0 topics is not allowed for " + name);

        this.worker = new Thread(this::StartReceiving,name);
        this.worker.start();

    }

    @Override
    public void close(){
        worker.interrupt();
    }

    public void StartReceiving() {
        LOGGER.info("started loop");

        KafkaMessageReceiver receiver = new KafkaMessageReceiver(kafka, inputTopics);
        receiver.start();

        while (true){
            try {
                MessageEnvelope env = receiver.receive();

                if(env == null)
                    continue;

                LOGGER.debug("Dispatch to router {}", router.getClass().getSimpleName());
                router.dispatch(env);
                receiver.commit(env);
                LOGGER.debug("Dispatch completed");

            } catch (InterruptException e) {
                LOGGER.info("stopped loop");
                break;
            } catch (Exception e) {
                LOGGER.error("Message processing failed", e);
            }
        }
    }
}
