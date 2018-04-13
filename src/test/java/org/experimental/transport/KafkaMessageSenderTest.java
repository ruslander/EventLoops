package org.experimental.transport;

import org.experimental.Env;
import org.experimental.MessageEnvelope;
import org.experimental.lab.SingleNodeKafkaCluster;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class KafkaMessageSenderTest {
    @Test
    public void send_message_over_topic() {

        SingleNodeKafkaCluster cl = Env.CLUSTER;

        KafkaMessageSender sender = new KafkaMessageSender(cl.getKfkConnectionString());
        sender.start();

        MessageEnvelope envelope = new MessageEnvelope(
                UUID.randomUUID(),
                "b",
                new HashMap<>(),
                new Aaa()

        );
        sender.send(Arrays.asList("a"), envelope);

        List<String> messages = cl.readAllMessages("a");

        Assert.assertEquals(messages.size(), 1);

        System.out.println(messages.get(0));
    }

    public class Aaa{
    }
}
