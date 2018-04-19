package org.experimental.directions;

import org.experimental.runtime.EndpointId;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MessageDestinations {

    HashMap<Type, List<String>> destinations = new HashMap<>();

    public void registerEndpoint(String endpointId, Class<?> ... aClass) {
        for(Class<?> cl : aClass){
            ArrayList<String> routes = new ArrayList<>();

            if(!destinations.containsKey(cl)){
                destinations.put(cl, routes);
            }else {
                routes = (ArrayList<String>) destinations.get(cl);
            }

            String inputTopicName = new EndpointId(endpointId).getInputTopicName();
            routes.add(inputTopicName);
        }
    }

    public List<String> destinations(Class<?> type){
        if(destinations.containsKey(type))
            return destinations.get(type);

        return new ArrayList<>();
    }
}
