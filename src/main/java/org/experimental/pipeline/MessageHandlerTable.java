package org.experimental.pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class MessageHandlerTable {

    private Map<Class<?>, Supplier<HandleMessages<?>>> handlers = new HashMap<>();

    public HandleMessages<Object> getHandlers(Object message){

        if(handlers.containsKey(message.getClass())){
            Supplier<HandleMessages<?>> factory = handlers.get(message.getClass());
            return (HandleMessages<Object>) factory.get();
        }

        return null;
    }

    public <T> void addHandler(HandleMessages<T> handler, Class<T> c) {
        if(!handlers.containsKey(c)){
            handlers.put(c, () -> handler);
        }
    }

    public <T> void addHandler(Supplier<HandleMessages<T>> handler, Class<T> c) {
        if(!handlers.containsKey(c)){
            handlers.put(c, () -> handler.get());
        }
    }

/*
    public <T> void registerAs(Function<MessageBus, HandleMessages<T>> handler, Class<T> c) {
        //Object ooo = (Function<MessageBus, HandleMessages<Object>>)handler;
    }
*/

}
