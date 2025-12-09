package org.itmo.aggregator;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class SentenceLengthAggregator {
    private List<SentenceLength> sortedSentences = new ArrayList<>();

    public void parseArray(JSONArray array) {
        List<SentenceLength> newSentences = new ArrayList<>(array.length());

        for (int i = 0; i < array.length(); i++) {
            JSONObject obj = array.getJSONObject(i);
            String sentence = obj.getString("sentence");
            int length = obj.getInt("length");
            newSentences.add(new SentenceLength(sentence, length));
        }

        mergeSortedLists(newSentences);
    }

    private void mergeSortedLists(List<SentenceLength> newList) {
        List<SentenceLength> merged = new ArrayList<>(
                sortedSentences.size() + newList.size()
        );

        int i = 0;
        int j = 0;

        while (i < sortedSentences.size() && j < newList.size()) {
            if (sortedSentences.get(i).compareTo(newList.get(j)) <= 0) {
                merged.add(sortedSentences.get(i++));
            } else {
                merged.add(newList.get(j++));
            }
        }

        while (i < sortedSentences.size()) {
            merged.add(sortedSentences.get(i++));
        }

        while (j < newList.size()) {
            merged.add(newList.get(j++));
        }

        sortedSentences = merged;
    }

    public List<SentenceLength> getSortedSentences() {
        return sortedSentences;
    }

    public static class SentenceLength implements Comparable<SentenceLength> {
        private final String sentence;
        private final int length;

        public SentenceLength(String sentence, int length) {
            this.sentence = sentence;
            this.length = length;
        }

        public String getSentence() {
            return sentence;
        }

        public int getLength() {
            return length;
        }

        @Override
        public int compareTo(SentenceLength other) {
            return Integer.compare(other.length, this.length); // descending
        }

        @Override
        public String toString() {
            return "SentenceLength{" +
                    "sentence='" + sentence + '\'' +
                    ", length=" + length +
                    '}';
        }
    }
}
