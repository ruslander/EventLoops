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

    public class TradingDayClosed{}

    @Test
    public void inbound() throws InterruptedException {
        String kfk = CLUSTER.getKafkaConnect();

        EndpointId endpointId = new EndpointId("trading-day-controller");

        KafkaMessageSender messageSender = new KafkaMessageSender(kfk);
        messageSender.start();

        MessageHandlerTable table = new MessageHandlerTable();
        UnicastRouter router = new UnicastRouter();

        MessagePipeline pipeline = new MessagePipeline(table, messageSender, endpointId, router);

        String loopName = "trading-day-controller";
        List<String> eventLoopTopic = Arrays.asList(loopName);
        try(ManagedEventLoop loop = new ManagedEventLoop(loopName, kfk, eventLoopTopic, pipeline)){

            MessageBus bus = pipeline.netMessageBus(null);
            bus.publish(new TradingDayClosed());

            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("trading-day-controller.events");
            Assert.assertEquals(messages.size(), 1);
        }
    }
}
