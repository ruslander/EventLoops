package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.runtime.EndpointWire;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

public class BusReplyTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"c3\"," +
            "\"type\":\"org.experimental.BusReplyTest$Ping\"," +
            "\"payload\":\"{}\"" +
            "}" +
            "}\n";

    public class Ping{}
    public class Pong{}

    @Test
    public void inbound() throws InterruptedException, IOException {

        try(EndpointWire wire = new EndpointWire("c1", CLUSTER.getKafkaConnect(),CLUSTER.getZookeeperString())){
            wire.registerHandler(Ping.class, bus -> message -> bus.reply(new Pong()));
            wire.configure();

            CLUSTER.sendMessages(new ProducerRecord<>("c1", env));

            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("c3");
            Assert.assertEquals(messages.size(), 1);
        }
    }
}
