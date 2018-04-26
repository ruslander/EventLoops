package org.experimental;

import org.experimental.runtime.EndpointWire;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

public class BusSendTest extends Env {

    public class TurnOff{}

    @Test
    public void inbound() throws InterruptedException, IOException {
        String kfk = CLUSTER.getKafkaConnect();
        String zk = CLUSTER.getZookeeperString();

        try(EndpointWire wire = new EndpointWire("stove-control-panel", kfk, zk)){
            wire.registerEndpointRoute("stove-owen", TurnOff.class);
            wire.configure();

            MessageBus bus = wire.getMessageBus();
            bus.send(new TurnOff());

            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("stove-owen");
            Assert.assertEquals(messages.size(), 1);
        }
    }
}
