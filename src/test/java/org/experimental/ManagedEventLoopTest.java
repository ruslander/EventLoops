package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.lab.SingleNodeKafkaCluster;
import org.experimental.pipeline.DispatchMessagesToHandlers;
import org.experimental.recoverability.ManagedEventLoop;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ManagedEventLoopTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"\"," +
            "\"type\":\"org.experimental.ManagedEventLoopTest$Ping\"," +
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
        DispatchMessagesToHandlers handlers = message -> {
            cnt.getAndIncrement();
            System.out.println("intercept ****** " + message.getUuid());
        };

        try(ManagedEventLoop loop = new ManagedEventLoop(loopName, kfk, inputTopics, handlers)){

            loop.start();

            CLUSTER.sendMessages(new ProducerRecord<>("c1", env));

            Thread.sleep(3000);

            Assert.assertEquals(cnt.get(), 1);
        }
    }
}
