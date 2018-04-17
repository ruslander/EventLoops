package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.lab.SingleNodeKafkaCluster;
import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.pipeline.MessageRouter;
import org.experimental.pipeline.RouteMessagesToHandlers;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BusShapingTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"\"," +
            "\"type\":\"org.experimental.BusShapingTest$Ping\"," +
            "\"payload\":\"{}\"" +
            "}" +
            "}\n";

    public class Ping{}

    @Test
    public void inbound() throws InterruptedException {
        String loopName = "c1";
        String kfk = CLUSTER.getKafkaConnect();
        List<String> inputTopics = Arrays.asList("c1");

        AtomicInteger cnt = new AtomicInteger();

        MessageHandlerTable table = new MessageHandlerTable();
        table.addHandler(Ping.class, bus -> message -> {
            cnt.getAndIncrement();
            System.out.println("intercept ****** " + message);

        });

        MessageRouter router = new MessageRouter(table);

        try(ManagedEventLoop loop = new ManagedEventLoop(loopName, kfk, inputTopics, router)){

            CLUSTER.sendMessages(new ProducerRecord<>("c1", env));

            Thread.sleep(3000);

            Assert.assertEquals(cnt.get(), 1);
        }
    }
}
