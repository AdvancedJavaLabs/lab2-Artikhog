package org.itmo;

import org.itmo.kafka.ReplaceWordConsumer;
import org.itmo.kafka.SentenceProducer;
import org.itmo.kafka.WordCountConsumer;
import org.itmo.splitter.SentenceSplitter;

import java.io.IOException;


public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();

        String server = "localhost:9092";
        String topic = "sentences";
        int partitions = 5;
        SentenceProducer producer = new SentenceProducer(server, topic, partitions);

        SentenceSplitter splitter = new SentenceSplitter(producer);
        splitter.processFileStreaming("./data/HarryPotter_3.txt");

        producer.sendPoisonPill();
        producer.flush();
        System.out.println("All sentences sent");

        WordCountConsumer wc = new WordCountConsumer(
                server, "word-count", "wc-group", partitions,
                "./result/statistics.txt"
        );

        ReplaceWordConsumer rw = new ReplaceWordConsumer(server, "word-replace", "rw-group");

        rw.startConsuming();
        wc.startConsuming();

        long end = System.currentTimeMillis();
        System.out.println("Execution time: " + (end - start) + " ms");
    }
}
