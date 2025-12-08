package org.itmo.aggregator;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SentenceLengthAggregator {
    private final List<SentenceLength> sortedSentences = new ArrayList<>();

    public void parseArray(JSONArray array) {
        for (int i = 0; i < array.length(); i++) {
            JSONObject obj = array.getJSONObject(i);

            String sentence = obj.getString("sentence");
            int length = obj.getInt("length");

            sortedSentences.add(new SentenceLength(sentence, length));
        }

        Collections.sort(sortedSentences);
    }

    public List<SentenceLength> getAll() {
        return Collections.unmodifiableList(sortedSentences);
    }

    public List<SentenceLength> getSortedSentences() {
        return sortedSentences;
    }

    public void printAll() {
        sortedSentences.forEach(System.out::println);
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
