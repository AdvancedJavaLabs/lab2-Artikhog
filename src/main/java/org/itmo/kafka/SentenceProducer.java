package org.itmo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SentenceProducer {
    private final Producer<String, String> producer;
    private final String topic;
    private final int partitions;
    private int sendedSentences = 0;

    public SentenceProducer(String server, String topic, int partitions) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.partitions = partitions;
    }

    public void sendSentence(String sentence) {
        String key = "key-"  + this.sendedSentences % this.partitions;
        ProducerRecord<String, String> record = new ProducerRecord<>(this.topic, key, sentence);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("Send message: partition=%d, offset=%d, key=%s%n",
                        metadata.partition(), metadata.offset(), record.key());
            } else {
                System.err.printf("Error send: %s%n", exception.getMessage());
            }
        });
        this.sendedSentences++;
        if (this.sendedSentences % 10 == 0) {
            producer.flush();
        }
        if (this.sendedSentences % 50 == 0) {
            System.out.println("Sent " + this.sendedSentences + " sentences");
        }
    }

    public void sendPoisonPill() {
        String key = "POISON_PILL";
        String value = "END_OF_STREAM";
        for (int i = 0; i < this.partitions; i+=1) {
            ProducerRecord<String, String> record = new ProducerRecord<>(this.topic, i, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Send message: partition=%d, offset=%d, key=%s%n",
                            metadata.partition(), metadata.offset(), record.key());
                } else {
                    System.err.printf("Error send: %s%n", exception.getMessage());
                }
            });
        }
    }

    public void flush() {
        this.producer.flush();
    }
}
