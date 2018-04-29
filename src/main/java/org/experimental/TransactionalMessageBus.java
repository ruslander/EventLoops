package org.experimental;

public class TransactionalMessageBus implements MessageBus {

    private MessageBus inner;
    private UnitOfWork ouw;

    public TransactionalMessageBus(MessageBus inner, UnitOfWork ouw) {
        this.inner = inner;
        this.ouw = ouw;
    }

    @Override
    public void publish(Object message) {
        ouw.register(() -> {inner.publish(message);});
    }

    @Override
    public void send(Object message) {
        ouw.register(() -> {inner.send(message);});
    }

    @Override
    public void reply(Object message) {
        ouw.register(() -> {inner.reply(message);});
    }
}
