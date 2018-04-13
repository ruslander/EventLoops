package org.experimental.transport;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.experimental.MessageEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaMessageReceiver {
    private  KafkaConsumer<String, TransportRecord> consumer;
    private final Gson recordGson = new Gson();
    private final Properties props;
    private final List<String> topics;

    private MessageEnvelopeSerializer serializer = new MessageEnvelopeSerializer();

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageReceiver.class);

    public KafkaMessageReceiver(String broker, List<String> topics) {
        this.topics = topics;
        props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "10");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.experimental.transport.TransportRecordByteSerializer");
    }

    public void start() {
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);

        LOGGER.debug("Initialized receiver");
    }

    public void stop() {
        consumer.close();
    }

    public List<MessageEnvelope> receive() throws Exception {
        List<MessageEnvelope> result = new ArrayList<>();

        ConsumerRecords<String, TransportRecord> records = consumer.poll(500);
        for (ConsumerRecord<String, TransportRecord> record : records) {
            MessageEnvelope envelope = serializer.recordToEnvelope(record.value());
            result.add(envelope);
        }

        return result;
    }
}
