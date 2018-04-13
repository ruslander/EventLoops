package org.experimental.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class MessageHandlerTable {

    private Map<Class, List<HandleMessages<Object>>> handlers = new HashMap<>();

    public List<HandleMessages<Object>> getHandlers(Object message){

        if(handlers.containsKey(message.getClass())){
            return handlers.get(message.getClass());
        }

        return new ArrayList<>();
    }

    public <T> void addHandler(Supplier<HandleMessages<T>> handler, Class<T> c) {
        ArrayList<HandleMessages<Object>> routes = new ArrayList<>();

        if(!handlers.containsKey(c)){
            handlers.put(c, routes);
        }else {
            routes = (ArrayList<HandleMessages<Object>>)handlers.get(c);
        }

        routes.add((HandleMessages<Object>)handler);
    }

    public Map<Class, List<HandleMessages<Object>>> getHandlers() {
        return handlers;
    }
}
