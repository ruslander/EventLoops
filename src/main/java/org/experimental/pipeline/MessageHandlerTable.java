package org.experimental.pipeline;

import org.experimental.MessageBus;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class MessageHandlerTable {

    private Map<Class<?>, Function<MessageBus, HandleMessages<?>>> handlers = new HashMap<>();

    public HandleMessages<Object> getHandlers(MessageBus bus, Object message){

        if(handlers.containsKey(message.getClass())){
            Function<MessageBus, HandleMessages<?>> factory = handlers.get(message.getClass());
            return (HandleMessages<Object>) factory.apply(bus);
        }

        return null;
    }

    /*public <T> void addHandler(Class<T> c, HandleMessages<T> handler) {
        if(!handlers.containsKey(c)){
            handlers.put(c, () -> handler);
        }
    }

    public <T> void addHandler(Class<T> c, Supplier<HandleMessages<T>> handler) {
        if(!handlers.containsKey(c)){
            handlers.put(c, () -> handler.get());
        }
    }*/

    public <T> void addHandler(Class<T> c, Function<MessageBus, HandleMessages<T>> handler) {
        if(!handlers.containsKey(c)){
            handlers.put(c, messageBus -> handler.apply(messageBus));
        }
    }

/*
    public <T> void registerAs(Function<MessageBus, HandleMessages<T>> handler, Class<T> c) {
        //Object ooo = (Function<MessageBus, HandleMessages<Object>>)handler;
    }
*/

}
