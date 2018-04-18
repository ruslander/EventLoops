package org.experimental;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class UnicastRouter {

    HashMap<Type, List<String>> recipients = new HashMap<>();

    public void registerEndpoint(String endpointId, Class<?> ... aClass) {
        for(Class<?> cl : aClass){
            ArrayList<String> routes = new ArrayList<>();

            if(!recipients.containsKey(cl)){
                recipients.put(cl, routes);
            }else {
                routes = (ArrayList<String>)recipients.get(cl);
            }

            String inputTopicName = new EndpointId(endpointId).getInputTopicName();
            routes.add(inputTopicName);
        }
    }

    public List<String> destinations(Class<?> type){
        if(recipients.containsKey(type))
            return recipients.get(type);

        return new ArrayList<>();
    }
}
