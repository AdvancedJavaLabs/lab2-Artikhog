from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import argparse
from utils.name_replacer import NameReplacer


class KafkaReplaceWordProcessor:
    def __init__(self, bootstrap_servers: str = 'localhost:9092', replace_name: str = "harry", new_name: str = "man"):
        self.bootstrap_servers = bootstrap_servers
        self.name_replacer = NameReplacer(replace_name, new_name)
        self.consumer = None
        self.producer = None

    def start_consuming(self, topic: str = 'sentences', partition: int = None):
        """Запускает потребитель и начинает подсчет слов"""
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
            group_id='sentence-word-replacer',
            value_deserializer=lambda x: x.decode('utf-8'),
            key_deserializer=lambda x: x.decode('utf-8') if x else None
        )

        # Если указан конкретный раздел - подписываемся на него
        if partition is not None:
            topic_partition = TopicPartition(topic, partition)
            self.consumer.assign([topic_partition])
            print(f"Listening to partition: {partition}")
        else:
            # Иначе подписываемся на весь топик (все разделы)
            self.consumer.subscribe(topics=[topic])
            print("Listening to all partitions")

        try:
            for message in self.consumer:
                key = message.key
                value = message.value

                # Дополнительная информация о сообщении
                print(f"Partition: {message.partition}, Offset: {message.offset}, Key: {message.key}")

                # Проверяем poison pill
                if self._is_poison_pill(key, value):
                    print("=" * 50)
                    print("POISON PILL DETECTED! Processing completed.")
                    print("=" * 50)
                    self._send_poison_pill()
                    break

                self._process_message(value, message.key, message.partition)
        except KeyboardInterrupt:
            print("\nConsumer stopped by user")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()

    def _is_poison_pill(self, key: str, value: str) -> bool:
        """Проверяет, является ли сообщение poison pill"""
        return key == "POISON_PILL" and value == "END_OF_STREAM"

    def _process_message(self, sentence: str, key: str, partition: int):
        # Используем метод из класса SentenceWordCounter
        replacedSentence = self.name_replacer.replace_names(sentence)
        future = self.producer.send(
            'word-replace',
            value=replacedSentence,
            key=key
        )

        # Ждем подтверждения отправки
        try:
            future.get(timeout=10)
            print("Message successfully sent to 'word-replace' topic")
        except Exception as e:
            print(f"Failed to send statistics to 'word-count' topic: {e}")

    def _send_poison_pill(self):
        future = self.producer.send(
            'word-replace',
            value="END_OF_STREAM",
            key="POISON_PILL"
        )


        try:
            future.get(timeout=10)
            print("Message successfully sent to 'word-replace' topic")
        except Exception as e:
            print(f"Failed to send statistics to 'word-count' topic: {e}")


def main():
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description='Kafka Sentence Word Replace')
    parser.add_argument('--partition', type=int, help='Partition number to listen to')
    parser.add_argument('--topic', type=str, default='sentences', help='Kafka topic name')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--replace_name', type=str, default="harry")
    parser.add_argument('--new_name', type=str, default="man")

    args = parser.parse_args()

    processor = KafkaReplaceWordProcessor(
        bootstrap_servers=args.bootstrap_servers,
        replace_name=args.replace_name,
        new_name=args.new_name,
    )
    processor.start_consuming(topic=args.topic, partition=args.partition)

if __name__ == "__main__":
    main()
