package org.experimental.lab.pubsub;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Publisher extends Thread {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public Publisher(String topic, String broker) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("client.id", "DemoProducer");
        props.put("auto.commit.interval.ms", 100);
        props.put("linger.ms", 50);
        props.put("block.on.buffer.full", true);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    public void run() {
        int messageNo = 1;
        while (messageNo < 5) {
            String messageStr = "Message_" + messageNo;

            try {
                String key = Integer.toString(messageNo);
                producer.send(new ProducerRecord<>(topic,
                        key,
                        messageStr)).get();
                producer.flush();
                System.out.println(new Date() + " Sent message: (" + messageNo + ", " + messageStr + ")");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            ++messageNo;
        }
    }
}