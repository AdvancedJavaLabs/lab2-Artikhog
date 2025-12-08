from textblob import TextBlob
from collections import Counter
import re
from typing import List

class SentimentAggregator:
    def __init__(self):
        self.sentiment_counts = Counter()
        self.total_sentences = 0
        self.processed_messages = 0

    def process_message(self, message: str):
        sentences = self._split_into_sentences(message)

        for sentence in sentences:
            # Анализ тональности с помощью TextBlob
            blob = TextBlob(sentence)
            polarity = blob.sentiment.polarity

            # Определяем категорию
            if polarity > 0.1:
                sentiment = 'positive'
            elif polarity < -0.1:
                sentiment = 'negative'
            else:
                sentiment = 'neutral'

            self.sentiment_counts[sentiment] += 1
            self.total_sentences += 1

        self.processed_messages += 1

    def get_statistics(self):
        return dict(self.sentiment_counts)

    def _split_into_sentences(self, text: str) -> List[str]:
        sentences = re.split(r'[.!?]+', text)
        return [sentence.strip() for sentence in sentences if sentence.strip()]