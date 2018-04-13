package org.experimental;

import org.experimental.pipeline.HandleMessages;
import org.experimental.pipeline.MessageHandlerTable;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

public class RouteMessagesToHandlersTest {
    @Test
    public void route() {
    }

    @Test
    public void register_handlers2(){
        MessageHandlerTable table = new MessageHandlerTable();
        table.registerAs(messageBus ->  new PingHandler2(messageBus), Ping.class);
    }


    @Test
    public void register_handlers(){
        MessageHandlerTable table = new MessageHandlerTable();
        table.addHandler(() -> new PingHandler(), Ping.class);

        Map<Class, List<HandleMessages<Object>>> handlers = table.getHandlers();

        Assert.assertNotNull(handlers.get(Ping.class));
    }

    @Test
    public void get_for_no_handlers(){
        MessageHandlerTable table = new MessageHandlerTable();

        List<HandleMessages<Object>> handlers = table.getHandlers(new Ping());

        Assert.assertNotNull(handlers);
        Assert.assertEquals(handlers.size(), 0);
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
