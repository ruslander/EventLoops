package org.experimental;

import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.pipeline.MessagePipeline;
import org.experimental.transport.KafkaMessageSender;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class BusSendTest extends Env {

    public class TurnOff{}

    @Test
    public void inbound() throws InterruptedException {
        String kfk = CLUSTER.getKafkaConnect();

        String name = "stove-control-panel";
        EndpointId endpointId = new EndpointId(name);

        KafkaMessageSender messageSender = new KafkaMessageSender(kfk);
        messageSender.start();

        MessageHandlerTable handlers = new MessageHandlerTable();
        UnicastRouter router = new UnicastRouter();
        router.registerEndpoint("stove-owen", TurnOff.class);

        MessagePipeline pipeline = new MessagePipeline(handlers, messageSender, endpointId, router);


        List<String> eventLoopTopic = Arrays.asList(name);
        try(ManagedEventLoop loop = new ManagedEventLoop(name, kfk, eventLoopTopic, pipeline)){

            MessageBus messageBus = pipeline.netMessageBus(null);
            messageBus.send(new TurnOff());


            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("stove-owen");
            Assert.assertEquals(messages.size(), 1);
        }
    }
}
