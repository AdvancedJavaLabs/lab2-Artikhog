class SentenceLengthSorter:
    def __init__(self, top_n: int = 10,):
        self.top_n = top_n
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

        if len(self.sorted_sentences) > self.top_n:
            self.sorted_sentences = self.sorted_sentences[:self.top_n]

        self.sentence_count += 1

    def get_statistics(self):
        return self.sorted_sentences[:self.top_n]