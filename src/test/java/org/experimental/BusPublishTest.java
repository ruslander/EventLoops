package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.pipeline.HandleMessages;
import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.pipeline.MessagePipeline;
import org.experimental.transport.KafkaMessageSender;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class BusPublishTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"c2\"," +
            "\"type\":\"org.experimental.BusPublishTest$CloseTradingDay\"," +
            "\"payload\":\"{}\"" +
            "}" +
            "}\n";

    public class CloseTradingDay{}
    public class TradingDayClosed{}

    public class CloseTradingDayHandler implements HandleMessages<CloseTradingDay>{

        private MessageBus bus;

        public CloseTradingDayHandler(MessageBus bus) {
            this.bus = bus;
        }

        @Override
        public void handle(CloseTradingDay message) {
            bus.publish(new TradingDayClosed());
        }
    }

    @Test
    public void inbound() throws InterruptedException {
        String kfk = CLUSTER.getKafkaConnect();

        MessageHandlerTable table = new MessageHandlerTable();
        table.registerHandler(CloseTradingDay.class, bus -> new CloseTradingDayHandler(bus));

        EndpointId endpointId = new EndpointId("trading-day-controller");

        KafkaMessageSender messageSender = new KafkaMessageSender(kfk);
        messageSender.start();
        MessagePipeline router = new MessagePipeline(table, messageSender, endpointId, new UnicastRouter());

        String loopName = "trading-day-controller";
        List<String> eventLoopTopic = Arrays.asList(loopName);
        try(ManagedEventLoop loop = new ManagedEventLoop(loopName, kfk, eventLoopTopic, router)){

            CLUSTER.sendMessages(new ProducerRecord<>(loopName, env));

            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("trading-day-controller.events");
            Assert.assertEquals(messages.size(), 1);
        }
    }
}
