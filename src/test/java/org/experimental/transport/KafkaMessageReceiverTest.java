package org.experimental.transport;

import org.experimental.Env;
import org.experimental.MessageEnvelope;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class KafkaMessageReceiverTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"c3\"," +
            "\"type\":\"org.experimental.BusReplyTest$Ping\"," +
            "\"payload\":\"{}\"" +
            "}" +
            "}\n";

    @Test
    public void receive_message_from_topic() throws Exception {

        send("aaaaaaaa", env);

        List<String> subs = Arrays.asList("aaaaaaaa");

        KafkaMessageReceiver receiver = new KafkaMessageReceiver(CLUSTER.getKfkConnectionString(),subs);
        receiver.start();

        for (int i = 0; i < 5; i++) {
            try{
                MessageEnvelope messages = receiver.receive();
                Assert.assertNotNull(messages);

                System.out.println(messages + " " + i);

                receiver.commit(messages);

                break;
            }catch (AssertionError e){
                Thread.sleep(2000);
            }
        }
    }
}
