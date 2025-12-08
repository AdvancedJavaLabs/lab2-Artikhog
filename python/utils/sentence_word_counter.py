import re
import json
from typing import Dict, Any
import argparse
import datetime

class SentenceWordCounter:
    def __init__(self):
        self.total_words = 0
        self.sentence_count = 0
        self.consumer = None

    def process_message(self, sentence: str, partition: int):
        words = re.findall(r'\b\w+\b', sentence)
        word_count = len(words)

        self.total_words += word_count
        self.sentence_count += 1

    def get_statistics(self):
        return self.total_words