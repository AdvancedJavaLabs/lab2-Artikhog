package org.itmo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.itmo.aggregator.SentimentSentenceAggregator;
import org.itmo.aggregator.TopNWordsAggregator;
import org.itmo.file.ResultFileWriter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class WordCountConsumer {
    private final KafkaConsumer<String, String> consumer;
    private final AtomicLong totalWords = new AtomicLong(0);
    private final String topic;
    private final int partitions;
    private int receivedMessages = 0;
    private final TopNWordsAggregator topNWordsAggregator;
    private final SentimentSentenceAggregator sentimentSentenceAggregator;
    private final String outputFilePath;
    private final ResultFileWriter writer;

    public WordCountConsumer(String server, String topic, String groupId, int partitions, String outputFilePath) {
        this.topic = topic;
        this.partitions = partitions;
        this.topNWordsAggregator = new TopNWordsAggregator();
        this.sentimentSentenceAggregator = new SentimentSentenceAggregator();
        this.outputFilePath = outputFilePath;
        this.writer = new ResultFileWriter(outputFilePath);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void startConsuming() {
        try {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Subscribed to topic: " + topic);
            System.out.println("Starting to consume messages...");

            while (receivedMessages < partitions) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    processMessage(record);
                }
            }
        } catch (Exception e) {
            System.err.println("Error in consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
            printFinalStats();
        }
    }

    private void processMessage(ConsumerRecord<String, String> record) {
        try {
            String key = record.key();
            String value = record.value();

            System.out.printf("Received message - Partition: %d, Offset: %d, Key: %s%n\n",
                    record.partition(), record.offset(), key);
            processStatisticsMessage(value);
            receivedMessages += 1;
        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }

    private void processStatisticsMessage(String jsonValue) {
        try {
            JSONObject stats = new JSONObject(jsonValue);
            if (stats.has("total_words_counted")) {
                long wordsFromStats = stats.getLong("total_words_counted");

                totalWords.addAndGet(wordsFromStats);
            }

            if (stats.has("top_words")) {
                JSONArray topWordsArray = stats.getJSONArray("top_words");
                System.out.println("Top words array length: " + topWordsArray.length());
                System.out.println("Top words: " + topWordsArray.toString());
                this.topNWordsAggregator.updateTopWords(topWordsArray);
            } else {
                System.out.println("No top_words in message");
            }

            if (stats.has("sentiment_analysis")) {
                JSONObject sentimentJson = stats.getJSONObject("sentiment_analysis");
                System.out.println("Sentiment analysis: " + sentimentJson.toString());
                this.sentimentSentenceAggregator.updateSentimentStats(sentimentJson);
            } else {
                System.out.println("No sentiment_analysis in message");
            }
        } catch (Exception e) {
            System.err.println("Error parsing statistics message: " + e.getMessage());
        }
    }

    public void printSentimentStats() {
        Map<String, Double> percentages = this.sentimentSentenceAggregator.getSentimentPercentages();
        writer.writeToFile("=== SENTIMENT ANALYSIS ===");
        for (Map.Entry<String, Double> entry : percentages.entrySet()) {
            double percentage = percentages.getOrDefault(entry.getKey(), 0.0);
            writer.writeToFile("%-10s: (%5.1f%%)",
                    entry.getKey().toUpperCase(), percentage);
        }
        writer.writeToFile("==========================");
    }

    public void printTopWords(int topN) {
        List<Map.Entry<String, Integer>> topWords = this.topNWordsAggregator.getCurrentTopWords(topN);
        writer.writeToFile("=== TOP " + topN + " WORDS ===");
        for (int i = 0; i < topWords.size(); i++) {
            Map.Entry<String, Integer> entry = topWords.get(i);
            writer.writeToFile("%2d. %-15s : %d", i + 1, entry.getKey(), entry.getValue());
        }
        writer.writeToFile("==========================");
    }

    private void printFinalStats() {
        long words = totalWords.get();

        writer.writeToFile("\n" + "=".repeat(50));
        writer.writeToFile("FINAL STATISTICS");
        writer.writeToFile("=".repeat(50));
        writer.writeToFile("Total words counted: %d", words);
        writer.writeToFile("=".repeat(50));
        printTopWords(10);
        printSentimentStats();

        // Дополнительно выводим в консоль информацию о завершении
        System.out.println("Consumer finished. Results written to: " + outputFilePath);
    }

}
