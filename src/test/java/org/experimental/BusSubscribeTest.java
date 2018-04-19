package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.pipeline.MessagePipeline;
import org.experimental.runtime.EndpointId;
import org.experimental.runtime.EndpointWire;
import org.experimental.runtime.ManagedEventLoop;
import org.experimental.directions.MessageSubscriptions;
import org.experimental.directions.MessageDestinations;
import org.experimental.transport.KafkaMessageSender;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
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
    public void inbound() throws InterruptedException, IOException {

        try(EndpointWire wire = new EndpointWire("c1", CLUSTER.getKafkaConnect(), CLUSTER.getZookeeperString())){
            AtomicInteger cnt = new AtomicInteger();
            wire.subscribeToEndpoint("u1", Tick.class);
            wire.registerHandler(Tick.class, bus -> message -> cnt.incrementAndGet());
            wire.configure();

            CLUSTER.sendMessages(new ProducerRecord<>("u1.events", env));

            Thread.sleep(4000);

            Assert.assertEquals(cnt.get(), 1);
        }
    }
}
