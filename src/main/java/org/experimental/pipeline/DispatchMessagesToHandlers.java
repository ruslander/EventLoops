package org.experimental.pipeline;

import org.experimental.MessageEnvelope;

public interface DispatchMessagesToHandlers {
    void dispatch(MessageEnvelope message);
}
