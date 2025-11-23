package org.itmo.splitter;

import org.itmo.kafka.SentenceProducer;
import java.io.*;

public class SentenceSplitter {
    private final SentenceProducer producer;

    public SentenceSplitter(SentenceProducer producer) {
        this.producer = producer;
    }

    public void processFileStreaming(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists() || !file.canRead()) {
            throw new IOException("Файл недоступен: " + filePath);
        }

        StringBuilder currentSentence = new StringBuilder();
        int sentenceCount = 0;

        try (FileReader fr = new FileReader(file);
             BufferedReader br = new BufferedReader(fr)) {

            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    // Пустая строка - возможный разделитель предложений
                    if (currentSentence.length() > 0) {
                        processSentence(currentSentence.toString().trim());
                        currentSentence.setLength(0);
                        sentenceCount++;
                    }
                    continue;
                }

                currentSentence.append(line).append(" ");

                // Проверяем, закончилось ли предложение
                String text = currentSentence.toString();
                if (text.matches(".*[.!?]\\s*$")) {
                    processSentence(text.trim());
                    currentSentence.setLength(0);
                    sentenceCount++;
                }
            }

            // Обрабатываем последнее предложение, если осталось
            if (!currentSentence.isEmpty()) {
                processSentence(currentSentence.toString().trim());
                sentenceCount++;
            }
        }

        System.out.println("Sentences readed: " + sentenceCount);
    }

    protected void processSentence(String sentence) {
        producer.sendSentence(sentence);
    }
}