package org.experimental.lab.pubsub;

import org.experimental.Env;
import org.experimental.lab.SingleNodeKafkaCluster;
import org.experimental.lab.pubsub.Subscriber;
import org.experimental.lab.pubsub.Publisher;
import org.testng.annotations.Test;

public class PubSubTest
{

    //@Test
    public void configure_endpoint_topics() throws Exception {

        String component1 = "component1";
        SingleNodeKafkaCluster kafka = Env.CLUSTER;

        ///kafka.createTopic(component1);

        String address = kafka.getKfkConnectionString();

        Subscriber subscriberThread = new Subscriber(component1, address);
        Publisher publisherThread = new Publisher(component1, address);

        subscriberThread.start();
        publisherThread.start();

        Thread.sleep(4000);

        subscriberThread.interrupt();
        publisherThread.interrupt();

        System.out.println("ready to go");
    }
}
