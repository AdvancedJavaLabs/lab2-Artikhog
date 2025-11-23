package org.itmo;

import org.itmo.kafka.SentenceProducer;
import org.itmo.kafka.WordCountConsumer;
import org.itmo.splitter.SentenceSplitter;

import java.io.IOException;


public class Main {
    public static void main(String[] args) {
        String server = "localhost:9092";
        String topic = "sentences";
        SentenceProducer producer = new SentenceProducer(server, topic, 3);

        SentenceSplitter splitter = new SentenceSplitter(producer);
        try {
            splitter.processFileStreaming("./data/HarryPotter.txt");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        producer.sendPoisonPill();
        producer.flush();
        System.out.println("All sentences sended");


        String groupId = "word-count-consumer-group";
        WordCountConsumer consumer = new WordCountConsumer(server, "word-count", groupId, 3);
        consumer.startConsuming();
    }
}
