package org.experimental.diagnostics;

import org.experimental.directions.MessageDestinations;
import org.experimental.directions.MessageSubscriptions;
import org.experimental.pipeline.MessageHandlerTable;
import org.experimental.runtime.EndpointId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ConfigurationInspector {
    private EndpointId endpointId;
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationInspector.class);

    private StringBuilder detailsAcc = new StringBuilder();

    public ConfigurationInspector(EndpointId endpointId) {
        this.endpointId = endpointId;

        detailsAcc.append("\n Endpoint Name: " + endpointId);
    }

    public void present() {
        LOGGER.info(detailsAcc.toString());
    }

    public void inspectHandlers(MessageHandlerTable table) {
        detailsAcc.append("\n\t Handles wired:");

        for (Class<?> cl: table.handlers.keySet()) {
            detailsAcc.append("\n\t\t " + cl.getSimpleName());
        }
    }

    public void inspectSubscriptions(MessageSubscriptions subscriptions) {
        detailsAcc.append("\n\t Event Subscriptions:");

        for (Map.Entry<String, List<Class<?>>> cl: subscriptions.sources.entrySet()) {
            List<String> types = cl.getValue().stream().map(x -> x.getSimpleName()).collect(Collectors.toList());
            detailsAcc.append("\n\t\t " + cl.getKey() + " " + String.join(",", types) );
        }
    }

    public void inspectRouting(MessageDestinations router) {
        detailsAcc.append("\n\t Command Routing:");

        for (Map.Entry<Class<?>, List<String>> cl: router.destinations.entrySet()) {
            detailsAcc.append("\n\t\t " + String.join(",", cl.getValue()) + " " +cl.getKey().getSimpleName());
        }
    }
}
