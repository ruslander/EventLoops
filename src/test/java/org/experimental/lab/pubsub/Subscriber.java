package org.experimental.lab.pubsub;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class Subscriber extends Thread {
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public Subscriber(String topic, String broker) {
        super();

        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("max.poll.records", String.valueOf(Integer.MAX_VALUE));

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;

        consumer.subscribe(Collections.singletonList(this.topic));
    }

    @Override
    public void run() {
        ConsumerRecords<String, String> records = consumer.poll(500);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(new Date() + " Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }
    }
}
