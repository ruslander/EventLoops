package org.experimental.directions;

import org.experimental.EndpointId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MessageSubscriptions {
    HashMap<String, List<Class<?>>> sources = new HashMap<>();

    public void subscribeToEndpoint(String endpointId, Class<?> ... aClass) {
        ArrayList<Class<?>> routes = new ArrayList<>();

        for(Class<?> cl : aClass){
            routes.add(cl);
        }

        String inputTopicName = new EndpointId(endpointId).getEventsTopicName();
        sources.put(inputTopicName, routes);
    }

    public List<String> sources(){
        return new ArrayList<>(sources.keySet());
    }
}
