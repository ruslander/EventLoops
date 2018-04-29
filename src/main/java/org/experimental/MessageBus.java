package org.experimental;

public interface MessageBus {
    void publish(Object message);

    void send(Object message);

    void reply(Object message);
}
