package org.experimental;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MessageBuilder {
    private String localAddress;

    public MessageBuilder(String localAddress) {
        this.localAddress = localAddress;
    }

    public MessageEnvelope buildMessage(Map<String, String> headers, Object message) {
        return new MessageEnvelope(
                UUID.randomUUID(),
                localAddress,
                headers,
                message
        );
    }

    public MessageEnvelope buildMessage(Object message) {
        return new MessageEnvelope(
                UUID.randomUUID(),
                localAddress,
                new HashMap<>(),
                message
        );
    }
}
