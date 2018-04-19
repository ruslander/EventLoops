package org.experimental;

import org.experimental.runtime.EndpointWire;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

public class BusPublishTest extends Env {

    public class TradingDayClosed{}

    @Test
    public void inbound() throws InterruptedException, IOException {

        try(EndpointWire wire = new EndpointWire("trading-day-controller", CLUSTER.getKafkaConnect())){
            wire.configure();

            MessageBus bus = wire.getMessageBus();
            bus.publish(new TradingDayClosed());

            Thread.sleep(4000);

            List<String> messages = CLUSTER.readAllMessages("trading-day-controller.events");
            Assert.assertEquals(messages.size(), 1);
        }
    }
}
