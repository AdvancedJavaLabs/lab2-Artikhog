## создание топика
```
kafka-topics --create --topic sentences --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## проверка, что топик создался
```
kafka-topics --list --bootstrap-server localhost:9092
```


## Проверка сообщений в топике и разделе
```
kafka-console-consumer --topic sentences --partition 1 --from-beginning --bootstrap-server localhost:9092
```

## Проверка всех разделов в топике
```
kafka-console-consumer --topic sentences --from-beginning --bootstrap-server localhost:9092 --property print.partition=true
```

## Чтение всех сообщений из топика
```
kafka-console-consumer --topic sentences --from-beginning --bootstrap-server localhost:9092
```