package org.experimental;

import org.experimental.runtime.EndpointWire;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class FlrAndSlrHandlerTest extends Env {

    private String env = "{" +
            "\"uuid\":\"9fb046d0-4318-4f2e-8ec3-0152449ebe7d\"," +
            "\"headers\":{}," +
            "\"content\":{" +
            "\"returnAddress\":\"c3\"," +
            "\"type\":\"org.experimental.FlrAndSlrHandlerTest$Ping\"," +
            "\"payload\":\"{}\"" +
            "}" +
            "}\n";

    public class Ping{}


    @Test
    public void flr_3_topic() throws InterruptedException, IOException {

        AtomicInteger cnt = new AtomicInteger();

        try(EndpointWire wire = wire("at3")){
            wire.registerHandler(Ping.class, bus -> message -> {
                cnt.incrementAndGet();
                throw new RuntimeException("intentional");
            });
            wire.configure();

            send("at3", env);

            Thread.sleep(6000);

            Assert.assertEquals(cnt.get(), 5);
            Assert.assertEquals(countMessages("at3.errors"), 1);
        }
    }

    @Test
    public void attempt_2_succeeds_topic() throws Exception {

        AtomicInteger cnt = new AtomicInteger();

        try(EndpointWire wire = wire("at2")){
            wire.registerHandler(Ping.class, bus -> message -> {
                int val = cnt.incrementAndGet();
                if(val < 3) throw new RuntimeException("intentional");
            });
            wire.configure();

            send("at2", env);

            Thread.sleep(4000);
        }

        Assert.assertEquals(cnt.get(), 3);
        Assert.assertEquals(countMessages("at2.error"), 1);
    }
}
