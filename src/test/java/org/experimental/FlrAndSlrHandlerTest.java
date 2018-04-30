package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.pipeline.HandleMessages;
import org.experimental.runtime.EndpointWire;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FlrAndSlrHandlerTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"c3\"," +
            "\"type\":\"org.experimental.FlrAndSlrHandlerTest$Ping\"," +
            "\"payload\":\"{}\"" +
            "}" +
            "}\n";

    public class Ping{}


    @Test
    public void flr_3_topic() throws InterruptedException, IOException {

        AtomicInteger cnt = new AtomicInteger();

        try(EndpointWire wire = new EndpointWire("at3", CLUSTER.getKafkaConnect(),CLUSTER.getZookeeperString())){
            wire.registerHandler(Ping.class, bus -> message -> {
                cnt.incrementAndGet();
                throw new RuntimeException("intentional");
            });
            wire.configure();

            CLUSTER.sendMessages(new ProducerRecord<>("at3", env));

            Thread.sleep(4000);

            Assert.assertEquals(cnt.get(), 3);
            Assert.assertEquals(CLUSTER.readAllMessages("at3.slr").size(), 1);
        }
    }

    @Test
    public void attempt_2_succeeds_topic() throws Exception {

        AtomicInteger cnt = new AtomicInteger();

        try(EndpointWire wire = new EndpointWire("at2", CLUSTER.getKafkaConnect(),CLUSTER.getZookeeperString())){
            wire.registerHandler(Ping.class, bus -> message -> {
                int val = cnt.incrementAndGet();
                if(val < 3) throw new RuntimeException("intentional");
            });
            wire.configure();

            CLUSTER.sendMessages(new ProducerRecord<>("at2", env));

            Thread.sleep(4000);

        }

        Assert.assertEquals(cnt.get(), 3);
        Assert.assertEquals(CLUSTER.readAllMessages("at2.slr").size(), 0);
    }
}
