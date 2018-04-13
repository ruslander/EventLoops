package org.experimental.pipeline;

import org.experimental.MessageEnvelope;

public interface RouteMessagesToHandlers {
    void Route(MessageEnvelope message);
}
