package org.itmo;

import org.itmo.kafka.ReplaceWordConsumer;
import org.itmo.kafka.SentenceProducer;
import org.itmo.kafka.WordCountConsumer;
import org.itmo.splitter.SentenceSplitter;

import java.io.IOException;


public class Main {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String server = "localhost:9092";
        String topic = "sentences";
        int partitions = 1;
        SentenceProducer producer = new SentenceProducer(server, topic, partitions);

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
        WordCountConsumer consumer = new WordCountConsumer(server, "word-count", groupId, partitions, "./result/HarryPotter_result.txt");
        consumer.startConsuming();
        String groupId2 = "word-replace-consumer-group";
        ReplaceWordConsumer replaceWordConsumer = new ReplaceWordConsumer(server, "word-replace", groupId2);
        replaceWordConsumer.startConsuming();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("Execution time: " + duration + " ms");
    }
}
