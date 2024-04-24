package jpoint2024;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class Producer {
    private final String topic;
    private final Random random;

    private final KafkaProducer<String, String> producer;

    public Producer(String topic) {
        this.topic = topic;
        this.random = new Random();

        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(properties);
    }

    public void push() {
        var key = String.valueOf(random.nextInt(10));
        var value = "some value";

        producer.send(new ProducerRecord<>(topic, key, value));
    }
}
