package org.itmo;

import org.itmo.kafka.ConsumerRunner;
import org.itmo.kafka.ReplaceWordConsumer;
import org.itmo.kafka.SentenceProducer;
import org.itmo.kafka.WordCountConsumer;
import org.itmo.splitter.SentenceSplitter;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();

        String server = "localhost:9092";
        String topic = "sentences";

        SentenceProducer producer = new SentenceProducer(server, topic, 1);

        SentenceSplitter splitter = new SentenceSplitter(producer);
        splitter.processFileStreaming("./data/HarryPotter.txt");

        producer.sendPoisonPill();
        producer.flush();
        System.out.println("All sentences sent");

        WordCountConsumer wc = new WordCountConsumer(
                server, "word-count", "wc-group", 1,
                "./result/HarryPotter_result.txt"
        );

        ReplaceWordConsumer rw = new ReplaceWordConsumer(
                server, "word-replace", "rw-group"
        );

        ExecutorService executor = Executors.newFixedThreadPool(2);

        Future<Void> wcFuture = executor.submit(new ConsumerRunner("WordCount", wc::startConsuming));
        Future<Void> rwFuture = executor.submit(new ConsumerRunner("ReplaceWord", rw::startConsuming));

        executor.shutdown();

        try {
            wcFuture.get();
            rwFuture.get();
        } catch (Exception e) {
            System.err.println("A consumer failed: " + e.getMessage());
            e.printStackTrace();
        }

        if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
            System.err.println("Consumers did not finish in time — forcing shutdown...");
            executor.shutdownNow();
        }

        long end = System.currentTimeMillis();
        System.out.println("Execution time: " + (end - start) + " ms");
    }
}
