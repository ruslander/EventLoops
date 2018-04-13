package org.experimental.transport;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TransportRecord {
    private final UUID uuid;
    private final Map<String, String> headers;
    private final Map<String, String> content;

    public TransportRecord(UUID uuid, Map<String, String> content, Map<String, String> headers) {
        this.uuid = uuid;
        this.content = content;
        this.headers = headers;
    }

    public TransportRecord(UUID uuid, Map<String, String> content) {
        this(uuid, content, new HashMap<>());
    }

    public UUID getUuid() {
        return uuid;
    }

    public Map<String, String> getContent() {
        return content;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }
}
