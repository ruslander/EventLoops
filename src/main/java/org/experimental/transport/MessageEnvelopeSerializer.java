package org.experimental.transport;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.experimental.MessageEnvelope;

import java.util.HashMap;
import java.util.Map;

public class MessageEnvelopeSerializer {
    private Gson gson = new Gson();

    public ProducerRecord<String, TransportRecord> envelopeToRecord(MessageEnvelope envelope, String topic) {
        HashMap<String, String> content = new HashMap<>();
        content.put("type", envelope.getLocalMessage().getClass().getName());
        content.put("payload", gson.toJson(envelope.getLocalMessage()));
        content.put("returnAddress", envelope.getReturnAddress());

        TransportRecord transportRecord = new TransportRecord(
                envelope.getUuid(),
                content,
                envelope.getHeaders()
        );

        return new ProducerRecord<>(topic, transportRecord);
    }

    public MessageEnvelope recordToEnvelope(TransportRecord record1) throws ClassNotFoundException {
        Map<String, String> content = record1.getContent();

        String returnAddress = content.get("returnAddress");
        String payload = content.get("payload");
        Class type = Class.forName(content.get("type"));

        Object localMessage = gson.fromJson(payload, type);

        return new MessageEnvelope(
                record1.getUuid(),
                returnAddress,
                record1.getHeaders(),
                localMessage);
    }
}
