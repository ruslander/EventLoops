package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.pipeline.HandleMessages;
import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.pipeline.MessageRouter;
import org.experimental.transport.KafkaMessageSender;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BusReplyTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"c2\"," +
            "\"type\":\"org.experimental.BusReplyTest$Ping\"," +
            "\"payload\":\"{}\"" +
            "}" +
            "}\n";

    public class Ping{}
    public class Pong{}

    public class PingHandler implements HandleMessages<Ping>{

        private MessageBus bus;

        public PingHandler(MessageBus bus) {
            this.bus = bus;
        }

        @Override
        public void handle(Ping message) {
            bus.reply(new Pong());
        }
    }

    @Test
    public void inbound() throws InterruptedException {
        String loopName = "c1";
        String kfk = CLUSTER.getKafkaConnect();
        List<String> inputTopics = Arrays.asList("c1");


        MessageHandlerTable table = new MessageHandlerTable();
        table.registerHandler(Ping.class, bus -> new PingHandler(bus));

        EndpointId endpointId = new EndpointId("c1");

        KafkaMessageSender messageSender = new KafkaMessageSender(kfk);
        messageSender.start();
        MessageRouter router = new MessageRouter(table, messageSender, endpointId);

        try(ManagedEventLoop loop = new ManagedEventLoop(loopName, kfk, inputTopics, router)){

            CLUSTER.sendMessages(new ProducerRecord<>("c1", env));

            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("c2");
            Assert.assertEquals(messages.size(), 1);
        }
    }
}
