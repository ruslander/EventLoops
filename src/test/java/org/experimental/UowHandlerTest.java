package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.pipeline.HandleMessages;
import org.experimental.runtime.EndpointWire;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

public class UowHandlerTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"c3\"," +
            "\"type\":\"org.experimental.UowHandlerTest$Ping\"," +
            "\"payload\":\"{}\"" +
            "}" +
            "}\n";

    public class Ping{}
    public class Pong{}

    public class PingUowHandler implements HandleMessages<Ping>{
        private MessageBus bus;

        public PingUowHandler(MessageBus bus) {
            this.bus = bus;
        }

        @Override
        public void handle(Ping message) {
            bus.publish(new Pong());

            throw new RuntimeException("this should make unit of work abort");
        }
    }

    @Test
    public void inbound() throws InterruptedException, IOException {

        try(EndpointWire wire = new EndpointWire("uow", CLUSTER.getKafkaConnect(),CLUSTER.getZookeeperString())){
            wire.registerHandler(Ping.class, bus -> new PingUowHandler(bus));
            wire.configure();

            CLUSTER.sendMessages(new ProducerRecord<>("uow", env));

            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("uow.events");
            Assert.assertEquals(messages.size(), 0);
        }
    }

    @Test
    public void error_topic() throws InterruptedException, IOException {

        try(EndpointWire wire = new EndpointWire("uow", CLUSTER.getKafkaConnect(),CLUSTER.getZookeeperString())){
            wire.registerHandler(Ping.class, bus -> new PingUowHandler(bus));
            wire.configure();

            CLUSTER.sendMessages(new ProducerRecord<>("uow", env));

            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("uow.errors");
            Assert.assertEquals(messages.size(), 1);
        }
    }
}
