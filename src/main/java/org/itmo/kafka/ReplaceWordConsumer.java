package org.itmo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ReplaceWordConsumer {
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private volatile boolean takeEndMessage = false;

    private final Map<Integer, String> messageStore = new ConcurrentHashMap<>();
    private int expectedNextIndex = 0;
    private final String outputFileName = "./result/result.txt";
    private BufferedWriter writer;

    public ReplaceWordConsumer(String server, String topic, String groupId) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        this.consumer = new KafkaConsumer<>(props);
        initializeWriter(); // Инициализируем writer в конструкторе
    }

    private void initializeWriter() {
        try {
            // Создаем директорию, если её нет
            Path resultDir = Paths.get("./result");
            if (!Files.exists(resultDir)) {
                Files.createDirectories(resultDir);
            }

            // Создаем или перезаписываем файл
            writer = new BufferedWriter(new FileWriter(outputFileName, false));
            System.out.println("Output file initialized: " + outputFileName);
        } catch (IOException e) {
            System.err.println("Error initializing output file: " + e.getMessage());
            throw new RuntimeException("Failed to initialize writer", e);
        }
    }

    public void startConsuming() {
        try {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Subscribed to topic: " + topic);
            System.out.println("Starting to consume messages...");

            while (!takeEndMessage) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    processMessage(record);
                }
            }
        } catch (Exception e) {
            System.err.println("Error in consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            closeResources();
        }
    }

    private void processMessage(ConsumerRecord<String, String> record) {
        try {
            String key = record.key();
            String value = record.value();

            System.out.printf("Received message - Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                    record.partition(), record.offset(), key, value);

            if ("POISON_PILL".equalsIgnoreCase(key)) {
                takeEndMessage = true;
                System.out.println("Received end signal. Finishing consumption...");
                // Записываем все оставшиеся сообщения перед завершением
                writeAllAvailableMessages();
                return; // Прекращаем обработку, следующая итерация цикла завершит работу
            }

            // Парсим номер из ключа (формат: key-<number>)
            if (key != null && key.startsWith("key-")) {
                int messageIndex = extractIndexFromKey(key);

                if (messageIndex >= 0) {
                    // Сохраняем сообщение в хранилище
                    messageStore.put(messageIndex, value.replaceAll("^\"|\"$", ""));
                    System.out.println("Stored message with index: " + messageIndex);

                    // Пытаемся записать все доступные последовательные сообщения
                    writeSequentialMessages();
                }
            }

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private int extractIndexFromKey(String key) {
        try {
            // Извлекаем число из ключа формата "key-<number>"
            String numberPart = key.substring(4); // Пропускаем "key-"
            return Integer.parseInt(numberPart);
        } catch (Exception e) {
            System.err.println("Error parsing key: " + key + ", error: " + e.getMessage());
            return -1;
        }
    }

    private synchronized void writeSequentialMessages() throws IOException {
        // Синхронизируем метод, чтобы избежать гонок при записи
        // Пока есть следующее ожидаемое сообщение в хранилище
        while (messageStore.containsKey(expectedNextIndex)) {
            String message = messageStore.remove(expectedNextIndex);

            // Записываем сообщение и пробел
            writer.write(message);
            writer.write("\n"); // Добавляем пробел между предложениями

            System.out.println("Written message with index: " + expectedNextIndex + " - " + message);
            expectedNextIndex++;
        }
        writer.flush(); // Flush после записи группы сообщений
    }

    private synchronized void writeAllAvailableMessages() {
        try {
            System.out.println("Writing all available messages before exit...");

            // Сначала записываем последовательные сообщения
            writeSequentialMessages();

            // Затем все оставшиеся (если есть пропуски)
            List<Integer> sortedKeys = new ArrayList<>(messageStore.keySet());
            Collections.sort(sortedKeys);

            boolean hasGaps = false;
            for (Integer key : sortedKeys) {
                if (key >= expectedNextIndex) {
                    if (!hasGaps) {
                        System.out.println("Warning: Gaps detected in message sequence!");
                        hasGaps = true;
                    }

                    String message = messageStore.get(key);
                    writer.write(message);
                    writer.write(" ");
                    System.out.println("Written remaining message with index: " + key + " - " + message);
                }
            }

            if (hasGaps) {
                writer.write("\n\n--- WARNING: Some messages were missing in the sequence ---");
            }

            writer.flush();

        } catch (IOException e) {
            System.err.println("Error writing remaining messages: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void closeResources() {
        try {
            if (writer != null) {
                writer.close();
                System.out.println("Output file closed: " + outputFileName);
            }

            if (consumer != null) {
                consumer.close();
                System.out.println("Kafka consumer closed");
            }

            // Выводим статистику
            System.out.println("Final statistics:");
            System.out.println("Last written index: " + (expectedNextIndex - 1));
            System.out.println("Remaining messages in store: " + messageStore.size());

        } catch (IOException e) {
            System.err.println("Error closing resources: " + e.getMessage());
        }
    }
}
