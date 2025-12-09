import re
from collections import Counter

class TopNAggregator:
    def __init__(self, top_n: int = 10, consumer_count: int = 3):
        self.top_n = top_n
        self.consumer_count = consumer_count
        self.word_frequency = Counter()
        self.total_messages = 0

    def process_message(self, sentence: str):
        words = re.findall(r'\b[a-zA-Zа-яА-Я]+\b', sentence.lower())
        self.word_frequency.update(words)

    def get_statistics(self):
        top_words = self.word_frequency.most_common(self.top_n * self.consumer_count)
        return top_words
