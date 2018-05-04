package org.experimental.transport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.experimental.MessageEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaMessageReceiver {
    private  KafkaConsumer<String, TransportRecord> consumer;
    private final Properties props;
    private final List<String> topics;

    private MessageEnvelopeSerializer serializer = new MessageEnvelopeSerializer();

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageReceiver.class);

    public KafkaMessageReceiver(String broker, List<String> topics) {
        this.topics = topics;
        props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "1"); /* records to include in 1 poll */
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

    public MessageEnvelope receive() throws Exception {

        ConsumerRecords<String, TransportRecord> records = consumer.poll(500);

        for (ConsumerRecord<String, TransportRecord> record : records) {
            MessageEnvelope envelope = serializer.recordToEnvelope(record.value());
            envelope.setOffset(recordOffset(record));

            return envelope;
        }

        return null;
    }

    HashMap<TopicPartition, OffsetAndMetadata> recordOffset(ConsumerRecord<String, TransportRecord> record) {
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1));
        return offsets;
    }

    public void commit(MessageEnvelope message){
        consumer.commitSync((Map<TopicPartition, OffsetAndMetadata>) message.getOffset());
    }
}
