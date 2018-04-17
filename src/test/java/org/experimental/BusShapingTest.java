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

public class BusShapingTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"c2\"," +
            "\"type\":\"org.experimental.BusShapingTest$Ping\"," +
            "\"payload\":\"{}\"" +
            "}" +
            "}\n";

    public class Ping{}
    public class Pong{}

    public class PingHandler implements HandleMessages<Ping>{

        private MessageBus bus;
        private AtomicInteger cnt;

        public PingHandler(MessageBus bus, AtomicInteger cnt) {
            this.bus = bus;
            this.cnt = cnt;
        }

        @Override
        public void handle(Ping message) {
            cnt.incrementAndGet();
            bus.reply(new Pong());
        }
    }

    @Test
    public void inbound() throws InterruptedException {
        String loopName = "c1";
        String kfk = CLUSTER.getKafkaConnect();
        List<String> inputTopics = Arrays.asList("c1");

        AtomicInteger cnt = new AtomicInteger();

        MessageHandlerTable table = new MessageHandlerTable();
        table.registerHandler(Ping.class, bus -> new PingHandler(bus, cnt));

        EndpointId endpointId = new EndpointId("c1");

        KafkaMessageSender messageSender = new KafkaMessageSender(kfk);
        messageSender.start();
        MessageRouter router = new MessageRouter(table, messageSender, endpointId);

        try(ManagedEventLoop loop = new ManagedEventLoop(loopName, kfk, inputTopics, router)){

            CLUSTER.sendMessages(new ProducerRecord<>("c1", env));

            Thread.sleep(4000);

            Assert.assertEquals(cnt.get(), 1);
        }
    }
}
