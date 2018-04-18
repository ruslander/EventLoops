package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.pipeline.HandleMessages;
import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.pipeline.MessagePipeline;
import org.experimental.transport.KafkaMessageSender;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class BusReplyTest extends Env {

    public class Ping{}
    public class Pong{}

    @Test
    public void inbound() throws InterruptedException {
        String loopName = "c1";
        String kfk = CLUSTER.getKafkaConnect();
        List<String> inputTopics = Arrays.asList("c1");


        UnicastRouter router = new UnicastRouter();
        MessageHandlerTable table = new MessageHandlerTable();

        EndpointId endpointId = new EndpointId("c1");

        KafkaMessageSender messageSender = new KafkaMessageSender(kfk);
        messageSender.start();

        MessagePipeline pipeline = new MessagePipeline(table, messageSender, endpointId, router);

        try(ManagedEventLoop loop = new ManagedEventLoop(loopName, kfk, inputTopics, pipeline)){

            MessageEnvelope envelope = new MessageEnvelope(
                    UUID.randomUUID(),
                    "c2",
                    new HashMap<>(),
                    new Ping()
            );
            MessageBus bus = pipeline.netMessageBus(envelope);

            bus.reply(new Pong());

            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("c2");
            Assert.assertEquals(messages.size(), 1);
        }
    }
}
