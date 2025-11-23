package org.itmo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class WordCountConsumer {
    private final KafkaConsumer<String, String> consumer;
    private final AtomicLong totalWords = new AtomicLong(0);
    private final AtomicLong messageCount = new AtomicLong(0);
    private final String topic;
    private final int partitions;
    private int receivedMessages = 0;

    public WordCountConsumer(String server, String topic, String groupId, int partitions) {
        this.topic = topic;
        this.partitions = partitions;

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

            System.out.printf("Received message - Partition: %d, Offset: %d, Key: %s%n",
                    record.partition(), record.offset(), key);

            // Проверяем, является ли сообщение статистикой из вашего Python скрипта
            if ("STATISTICS".equals(key)) {
                processStatisticsMessage(value);
                receivedMessages += 1;
            }

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }

    private void processStatisticsMessage(String jsonValue) {
        try {
            JSONObject stats = new JSONObject(jsonValue);

            if (stats.has("total_words_counted")) {
                long wordsFromStats = stats.getLong("total_words_counted");
                long sentencesFromStats = stats.getLong("total_sentences_processed");

                 totalWords.addAndGet(wordsFromStats);
                 messageCount.addAndGet(sentencesFromStats);
            }
        } catch (Exception e) {
            System.err.println("Error parsing statistics message: " + e.getMessage());
        }
    }

    private void printFinalStats() {
        long words = totalWords.get();
        long messages = messageCount.get();

        System.out.println("\n" + "=".repeat(50));
        System.out.println("FINAL STATISTICS");
        System.out.println("=".repeat(50));
        System.out.printf("Total messages processed: %d%n", messages);
        System.out.printf("Total words counted: %d%n", words);
        System.out.println("=".repeat(50));
    }
}
