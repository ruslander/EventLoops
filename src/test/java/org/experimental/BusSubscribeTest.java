package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.pipeline.MessagePipeline;
import org.experimental.runtime.ManagedEventLoop;
import org.experimental.directions.MessageSubscriptions;
import org.experimental.directions.MessageDestinations;
import org.experimental.transport.KafkaMessageSender;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BusSubscribeTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"c3\"," +
            "\"type\":\"org.experimental.BusSubscribeTest$Tick\"," +
            "\"payload\":\"{}\"" +
            "}" +
            "}\n";

    public class Tick{}

    @Test
    public void inbound() throws InterruptedException {
        String kfk = CLUSTER.getKafkaConnect();

        AtomicInteger cnt = new AtomicInteger();

        MessageDestinations router = new MessageDestinations();

        MessageSubscriptions subscriptions = new MessageSubscriptions();
        subscriptions.subscribeToEndpoint("u1", Tick.class);

        MessageHandlerTable table = new MessageHandlerTable();
        table.registerHandler(Tick.class, bus -> message -> cnt.incrementAndGet());

        EndpointId endpointId = new EndpointId("c1");

        KafkaMessageSender messageSender = new KafkaMessageSender(kfk);
        messageSender.start();

        MessagePipeline pipeline = new MessagePipeline(table, messageSender, endpointId, router);

        String loopName = "d1.subscriptions";

        try(ManagedEventLoop loop = new ManagedEventLoop(loopName, kfk, subscriptions.sources(), pipeline)){

            CLUSTER.sendMessages(new ProducerRecord<>("u1.events", env));


            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("c2");
            Assert.assertEquals(cnt.get(), 1);
        }
    }
}
