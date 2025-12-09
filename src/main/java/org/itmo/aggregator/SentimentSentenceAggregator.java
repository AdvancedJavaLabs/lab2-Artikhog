package org.itmo.aggregator;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class SentimentSentenceAggregator {
    private final Map<String, Integer> sentimentStats;

    public SentimentSentenceAggregator() {
        this.sentimentStats = new HashMap<>();
    }

    public void updateSentimentStats(JSONObject sentimentJson) {
        // Обновляем статистику для каждой категории тональности
        for (String key : sentimentJson.keySet()) {
            int count = sentimentJson.getInt(key);
            sentimentStats.merge(key, count, Integer::sum);
        }
    }

    public Map<String, Double> getSentimentPercentages() {
        Map<String, Double> percentages = new HashMap<>();
        int total = sentimentStats.values().stream().mapToInt(Integer::intValue).sum();
        if (total > 0) {
            for (Map.Entry<String, Integer> entry : sentimentStats.entrySet()) {
                double percentage = (entry.getValue() * 100.0) / total;
                percentages.put(entry.getKey(), percentage);
            }
        }
        return percentages;
    }
}
