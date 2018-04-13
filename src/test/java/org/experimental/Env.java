package org.experimental;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.lab.SingleNodeKafkaCluster;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;
import java.util.List;

public class Env {

    public static SingleNodeKafkaCluster CLUSTER;

    @BeforeSuite
    public void boot() throws IOException {
        CLUSTER = new SingleNodeKafkaCluster();
        CLUSTER.startup();

        CLUSTER.sendMessages(new ProducerRecord<>("ready", "yes"));

        while (true){
            List<String> ready = CLUSTER.readAllMessages("ready");

            if(ready.size() != 0)
                break;
        }
    }

    @AfterSuite
    public void release(){
        CLUSTER.shutdown();
    }
}
