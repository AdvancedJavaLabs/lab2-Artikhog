from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import argparse
from utils.name_replacer import NameReplacer
from utils.sentence_word_counter import SentenceWordCounter
from utils.top_n_aggregator import TopNAggregator
from utils.sentimental_analysis import SentimentAggregator
from utils.sentence_length_sorter import SentenceLengthSorter


class KafkaMultiProcessor:
    def __init__(
            self,
            bootstrap_servers: str = 'localhost:9092',
            top_n: int = 10,
            consumer_count: int = 3,
            replace_name: str = "harry",
            new_name: str = "man"
    ):
        self.bootstrap_servers = bootstrap_servers

        self.word_counter = SentenceWordCounter()
        self.top_n_aggregator = TopNAggregator(top_n, consumer_count)
        self.sentiment_aggregator = SentimentAggregator()
        self.sentence_length_sorter = SentenceLengthSorter()

        self.name_replacer = NameReplacer(replace_name, new_name)

        self.consumer = None
        self.producer_stats = None
        self.producer_replace = None

        self.sent_replace_poison_pill = False

    def start_consuming(self, topic: str = 'sentences', partition: int = None):
        print("Starting Kafka Consumer...")

        self.producer_stats = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )

        self.producer_replace = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )

        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='sentence-multi-processor',
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

                # print(f"Partition: {message.partition}, Offset: {message.offset}, Key: {message.key}")

                if self._is_poison_pill(key, value):
                    print("=" * 50)
                    print("POISON PILL DETECTED! Processing completed.")
                    print("=" * 50)

                    self._send_final_stats()

                    if not self.sent_replace_poison_pill:
                        self._send_poison_pill_to_replace()
                        self.sent_replace_poison_pill = True

                    break

                self._process_message(value, message.key, message.partition)

        except KeyboardInterrupt:
            print("\nConsumer stopped by user")
            self._print_final_stats()
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer_stats:
                self.producer_stats.close()
            if self.producer_replace:
                self.producer_replace.close()

    def _is_poison_pill(self, key: str, value: str) -> bool:
        return key == "POISON_PILL" and value == "END_OF_STREAM"

    def _process_message(self, sentence: str, key: str, partition: int):
        self.word_counter.process_message(sentence, partition)
        self.top_n_aggregator.process_message(sentence)
        self.sentiment_aggregator.process_message(sentence)
        self.sentence_length_sorter.process_message(sentence)

        replaced_sentence = self.name_replacer.replace_names(sentence)
        self._send_to_replace_topic(replaced_sentence, key)

    def _send_to_replace_topic(self, sentence: str, key: str):
        future = self.producer_replace.send(
            'word-replace',
            value=sentence,
            key=key
        )

        try:
            future.get(timeout=10)
        except Exception as e:
            print(f"Failed to send message to 'word-replace' topic: {e}")

    def _send_poison_pill_to_replace(self):
        future = self.producer_replace.send(
            'word-replace',
            value="END_OF_STREAM",
            key="POISON_PILL"
        )

        try:
            future.get(timeout=10)
            # print("Poison pill successfully sent to 'word-replace' topic")
        except Exception as e:
            print(f"Failed to send poison pill to 'word-replace' topic: {e}")

    def _send_final_stats(self):
        total_words = self.word_counter.get_statistics()
        top_words = self.top_n_aggregator.get_statistics()
        sentiment_stats = self.sentiment_aggregator.get_statistics()
        sorted_sentences = self.sentence_length_sorter.get_statistics()

        stats_message = {
            "total_words_counted": total_words,
            "top_words": [{"word": word, "count": count} for word, count in top_words],
            "sentiment_analysis": sentiment_stats,
            "sorted_sentences": sorted_sentences,
            "processed_messages": self.sentiment_aggregator.processed_messages
        }

        future = self.producer_stats.send(
            'word-count',
            value=stats_message,
            key='STATISTICS'
        )

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

        print("\n" + "=" * 60)
        print("FINAL STATISTICS")
        print("=" * 60)
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

        print("\nSentence length sorted list:")
        for i, sentence_data in enumerate(sorted_sentences, 1):
            sentence = sentence_data['sentence']
            length = sentence_data['length']
            print(f"{i:2d}. [{length:2d} симв.] {sentence}")
        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Kafka Multi-Processor: Word Counter and Name Replacer')

    parser.add_argument('--partition', type=int, help='Partition number to listen to')
    parser.add_argument('--topic', type=str, default='sentences', help='Kafka topic name')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092',
                        help='Kafka bootstrap servers')

    parser.add_argument('--top-n', type=int, default=10,
                        help='Number of top words to track (default: 10)')
    parser.add_argument('--consumer-count', type=int, default=3,
                        help='Expected number of consumers (default: 3)')

    parser.add_argument('--replace_name', type=str, default="harry",
                        help='Name to replace in sentences (default: harry)')
    parser.add_argument('--new_name', type=str, default="man",
                        help='New name to use as replacement (default: man)')

    args = parser.parse_args()

    processor = KafkaMultiProcessor(
        bootstrap_servers=args.bootstrap_servers,
        top_n=args.top_n,
        consumer_count=args.consumer_count,
        replace_name=args.replace_name,
        new_name=args.new_name
    )
    processor.start_consuming(topic=args.topic, partition=args.partition)


if __name__ == "__main__":
    main()