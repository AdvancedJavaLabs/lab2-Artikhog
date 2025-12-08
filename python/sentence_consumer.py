from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import argparse
from utils.sentence_word_counter import SentenceWordCounter
from utils.top_n_aggregator import TopNAggregator
from utils.sentimental_analysis import SentimentAggregator
from utils.sentence_length_sorter import SentenceLengthSorter

class KafkaSentenceProcessor:
    def __init__(self, bootstrap_servers: str = 'localhost:9092', top_n: int = 10, consumer_count: int = 3):
        self.bootstrap_servers = bootstrap_servers
        self.word_counter = SentenceWordCounter()
        self.top_n_aggregator = TopNAggregator(top_n, consumer_count)
        self.sentiment_aggregator = SentimentAggregator()
        self.sentence_length_sorter = SentenceLengthSorter()
        self.consumer = None
        self.producer = None

    def start_consuming(self, topic: str = 'sentences', partition: int = None):
        print("Starting Kafka Consumer...")

        # Настройка продюсера
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )

        # Настройка потребителя
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='sentence-word-counter',
            value_deserializer=lambda x: x.decode('utf-8'),
            key_deserializer=lambda x: x.decode('utf-8') if x else None
        )

        if partition is not None:
            topic_partition = TopicPartition(topic, partition)
            self.consumer.assign([topic_partition])
            print(f"Listening to partition: {partition}")
        else:
            self.consumer.subscribe(topics=[topic])
            print("Listening to all partitions")

        try:
            for message in self.consumer:
                key = message.key
                value = message.value

                print(f"Partition: {message.partition}, Offset: {message.offset}")

                # Проверяем poison pill
                if self._is_poison_pill(key, value):
                    print("=" * 50)
                    print("POISON PILL DETECTED! Processing completed.")
                    print("=" * 50)
                    self._send_final_stats()
                    break

                self._process_message(value, message.partition)

        except KeyboardInterrupt:
            print("\nConsumer stopped by user")
            self._print_final_stats()
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()

    def _is_poison_pill(self, key: str, value: str) -> bool:
        return key == "POISON_PILL" and value == "END_OF_STREAM"

    def _process_message(self, sentence: str, partition: int):
        self.word_counter.process_message(sentence, partition)
        self.top_n_aggregator.process_message(sentence)
        self.sentiment_aggregator.process_message(sentence)
        self.sentence_length_sorter.process_message(sentence)

    def _send_final_stats(self):
        # Получение статистики
        total_words = self.word_counter.get_statistics()
        top_words = self.top_n_aggregator.get_statistics()
        sentiment_stats = self.sentiment_aggregator.get_statistics()
        sorted_sentences = self.sentence_length_sorter.get_statistics()

        stats_message = {
            "total_words_counted": total_words,
            "top_words": [{"word": word, "count": count} for word, count in top_words],
            "sentiment_analysis": sentiment_stats,
            # "sorted_sentences": sorted_sentences,
            "processed_messages": self.sentiment_aggregator.processed_messages
        }

        future = self.producer.send(
            'word-count',
            value=stats_message,
            key='STATISTICS'
        )

        #Подтверждение отправки
        try:
            future.get(timeout=10)
            print("Statistics successfully sent to 'word-count' topic")
            print("STATISTICS MESSAGE:")
            print(json.dumps(stats_message, indent=2))
        except Exception as e:
            print(f"Failed to send statistics to 'word-count' topic: {e}")

        self._print_final_stats()

    def _print_final_stats(self):
        total_word = self.word_counter.get_statistics()
        top_words = self.top_n_aggregator.get_statistics()
        sentiment_stats = self.sentiment_aggregator.get_statistics()
        sorted_sentences = self.sentence_length_sorter.get_statistics()

        print("FINAL STATISTICS")
        print("=" * 50)
        print(f"Total messages processed: {self.sentiment_aggregator.processed_messages}")
        print(f"Total words counted: {total_word}")
        print(f"Unique words found: {len(self.top_n_aggregator.word_frequency)}")
        print(f"Top {self.top_n_aggregator.top_n} words:")

        for i, (word, count) in enumerate(top_words[:self.top_n_aggregator.top_n], 1):
            print(f"  {i:2d}. {word}: {count}")

        print("\nSentiment analysis:")
        total_sentences = self.sentiment_aggregator.total_sentences
        for sentiment, count in sentiment_stats.items():
            percentage = (count / total_sentences * 100) if total_sentences > 0 else 0
            print(f"  {sentiment.capitalize()}: {count} sentences ({percentage:.1f}%)")
        print("=" * 60)

        print("\nSentence length sorted list:")
        for i, sentence_data in enumerate(sorted_sentences, 1):
            sentence = sentence_data['sentence']
            length = sentence_data['length']
            print(f"{i:2d}. [{length:2d} симв.] {sentence}")


def main():
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description='Kafka Sentence Word Counter with Top N Words')
    parser.add_argument('--partition', type=int, help='Partition number to listen to')
    parser.add_argument('--topic', type=str, default='sentences', help='Kafka topic name')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--top-n', type=int, default=10,
                        help='Number of top words to track (default: 10)')
    parser.add_argument('--consumer-count', type=int, default=3,
                        help='Expected number of consumers (default: 3)')

    args = parser.parse_args()

    # Создаем и запускаем процессор
    processor = KafkaSentenceProcessor(
        bootstrap_servers=args.bootstrap_servers,
        top_n=args.top_n,
        consumer_count=args.consumer_count
    )
    processor.start_consuming(topic=args.topic, partition=args.partition)


if __name__ == "__main__":
    main()