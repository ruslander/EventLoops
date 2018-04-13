package org.experimental;

public class EndpointId {
    private String id;

    public EndpointId(String id) {
        this.id = id;
    }

    public String getEventsTopicName() {
        return id + ".events";
    }

    public String getErrorsTopicName() {
        return id + ".errors";
    }

    public String getInputTopicName() {
        return id;
    }

    @Override
    public String toString() {
        return id;
    }
}
