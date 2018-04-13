package org.experimental.transport;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.Env;
import org.experimental.MessageEnvelope;
import org.experimental.lab.SingleNodeKafkaCluster;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class KafkaMessageReceiverTest {

    private String env = "{\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\",\"headers\":{},\"content\":{\"type\":\"org.experimental.KafkaMessageSenderTest$Aaa\",\"content\":\"{}\"}}\n";

    @Test
    public void receive_message_from_topic() throws Exception {

        SingleNodeKafkaCluster cl = Env.CLUSTER;
        cl.sendMessages(new ProducerRecord<>("aaaaaaaa", env));

        List<String> subs = Arrays.asList("aaaaaaaa");

        KafkaMessageReceiver subscriber = new KafkaMessageReceiver(cl.getKfkConnectionString(),subs);
        subscriber.start();

        for (int i = 0; i < 5; i++) {
            try{
                List<MessageEnvelope> messages = subscriber.receive();
                Assert.assertEquals(messages.size(), 1);

                System.out.println(messages.get(0) + " " + i);
                break;
            }catch (AssertionError e){
                Thread.sleep(2000);
            }
        }
    }
}
