package org.experimental.pipeline;

public interface HandleMessages<T> {
    void handle(T message);
}
