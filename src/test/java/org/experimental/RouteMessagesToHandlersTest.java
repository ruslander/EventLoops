package org.experimental;

import org.experimental.pipeline.HandleMessages;
import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.pipeline.MessageRouter;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class RouteMessagesToHandlersTest {

    MessageEnvelope envelope = new MessageEnvelope(UUID.randomUUID(), "", new HashMap<>(), new Ping());

    @Test
    public void when_no_handler_registered_will_noop(){
        MessageHandlerTable table = new MessageHandlerTable();
        MessageRouter router = new MessageRouter(table);

        router.Route(envelope);
    }

    @Test
    public void will_invoke_associated_hadler(){
        AtomicInteger cnt = new AtomicInteger(0);

        MessageHandlerTable table = new MessageHandlerTable();
        table.addHandler(Ping.class, messageBus -> message -> cnt.incrementAndGet());
        MessageRouter router = new MessageRouter(table);

        router.Route(envelope);

        Assert.assertEquals(cnt.get(), 1);
    }

    @Test
    public void no_handler_will_give_null(){
        MessageHandlerTable table = new MessageHandlerTable();
        HandleMessages<Object> hndl = table.getHandlers(new MessageBus(), new Ping());

        Assert.assertNull(hndl);
    }

    @Test
    public void will_get_registered_handler(){
        MessageHandlerTable table = new MessageHandlerTable();
        table.addHandler(Ping.class, messageBus -> new PingHandler());

        HandleMessages<Object> hndl = table.getHandlers(new MessageBus(), new Ping());

        Assert.assertNotNull(hndl);
    }

    @Test
    public void factory_check(){
        MessageHandlerTable table = new MessageHandlerTable();
        table.addHandler(Ping.class, messageBus ->  new PingHandler());

        Ping ping = new Ping();
        MessageBus bus = new MessageBus();

        Assert.assertNotEquals(table.getHandlers(bus, ping), table.getHandlers(bus, ping));
    }

    public class Ping{
    }

    public class PingHandler implements HandleMessages<Ping>{
        @Override
        public void handle(Ping message) {

        }
    }

    public class PingHandler2 implements HandleMessages<Ping>{
        private MessageBus messageBus;

        public PingHandler2(MessageBus messageBus) {
            this.messageBus = messageBus;
        }

        @Override
        public void handle(Ping message) {

        }
    }
}
