package org.itmo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

public class ReplaceWordConsumer {

    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    private final Map<Integer, String> messageStore = new HashMap<>();
    private int expectedNextIndex = 0;
    private BufferedWriter writer;

    private volatile boolean running = true;

    public ReplaceWordConsumer(String server, String topic, String groupId) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<>(props);
    }

    private void initWriter() throws IOException {
        Path path = Paths.get("./result");
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        writer = Files.newBufferedWriter(Paths.get("./result/replaced_text.txt"));
    }

    public void startConsuming() {
        try {
            initWriter();
            consumer.subscribe(Collections.singletonList(topic));

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    if ("POISON_PILL".equalsIgnoreCase(record.key())) {
                        running = false;
                        break;
                    }

                    processMessage(record);
                }
                writeSequentialMessages();
                consumer.commitAsync();
            }

        } catch (WakeupException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            shutdownCleanly();
        }
    }

    private void shutdownCleanly() {
        try {
            writeSequentialMessages();
            consumer.commitSync();
        } catch (Exception ignore) {
        }

        try {
            if (writer != null) writer.close();
        } catch (IOException ignored) {
        }

        consumer.close();
    }

    private void processMessage(ConsumerRecord<String, String> record) {
        String key = record.key();
        if (key != null && key.startsWith("key-")) {
            int index = parseIndex(key);
            if (index >= 0) {
                String cleaned = trimQuotes(record.value());
                messageStore.put(index, cleaned);
            }
        }
    }

    private int parseIndex(String key) {
        try {
            return Integer.parseInt(key.substring(4));
        } catch (Exception e) {
            return -1;
        }
    }

    private String trimQuotes(String s) {
        if (s == null || s.length() < 2) return s;
        if (s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private void writeSequentialMessages() throws IOException {
        while (messageStore.containsKey(expectedNextIndex)) {
            String msg = messageStore.remove(expectedNextIndex);
            writer.write(msg);
            writer.newLine();
            expectedNextIndex++;
        }
        writer.flush();
    }
}