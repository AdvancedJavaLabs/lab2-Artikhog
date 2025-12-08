import re
import json
from typing import Dict, Any
import argparse
import datetime


class SentenceLengthSorter:
    def __init__(self):
        self.sorted_sentences = []
        self.sentence_count = 0
        self.consumer = None

    def process_message(self, sentence: str):
        sentence_data = {
            'sentence': sentence,
            'length': len(sentence),
        }
        # Добавляем в список и сортируем по длине
        self.sorted_sentences.append(sentence_data)
        self.sorted_sentences.sort(key=lambda x: -x['length'])

        self.sentence_count += 1

    def get_statistics(self):
        return self.sorted_sentences