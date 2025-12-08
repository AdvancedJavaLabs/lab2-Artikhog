package org.itmo.aggregator;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopNWordsAggregator {
    private final Map<String, Integer> topWordsMap;

    public TopNWordsAggregator() {
        this.topWordsMap = new HashMap<>();
    }

    public void updateTopWords(JSONArray topWordsArray) {
        Map<String, Integer> incomingTopWords = new HashMap<>();

        for (int i = 0; i < topWordsArray.length(); i++) {
            JSONObject wordObj = topWordsArray.getJSONObject(i);
            String word = wordObj.getString("word");
            int count = wordObj.getInt("count");
            incomingTopWords.put(word, count);
        }

        updateTopWords(incomingTopWords);
    }

    public void updateTopWords(Map<String, Integer> incomingTopWords) {
        for (Map.Entry<String, Integer> entry : incomingTopWords.entrySet()) {
            String word = entry.getKey();
            int count = entry.getValue();
            // Обновляем счетчик для каждого слова
            topWordsMap.merge(word, count, Integer::sum);
        }

        System.out.println("Updated top words statistics. Total unique words: " + topWordsMap.size());
    }

    public List<Map.Entry<String, Integer>> getCurrentTopWords(int topN) {
        return topWordsMap.entrySet()
                .stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(topN)
                .collect(Collectors.toList());
    }
}
