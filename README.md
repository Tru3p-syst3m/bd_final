Задание: 
1. Приложение порождает бизнес-события (JMeter, FastStream) 
2. События публикуются в kafka 
3. Отдельные потребители их обрабатывают. 
4. Часть потока агрегируется через kafka Streams 
5. Данные из бд или в БД через kafka connect Пример событий: 
    - интернет магазин: 'OrderCreated', 'OrderPaid', 'OrderCancelled' 
    - спортцентр: 'TrainingBooked', 'SubscribtionExpired' 
    - транспорт: 'TripCreated', 'CrashDetected' 
    - библиотека: 'BookIssued', 'BookReturned' 

Требования: Producer 
- Минимум 3 типа событий
- Единый формат сообщений
- Наличие ключа сообщений
- Наличие метаданных 

Минимальная струтктура события: eventId, eventType, entetyId, timestamp, source, payload, version Consumer:
- Минимум 2 Consumer group
- Commit offset (manual/auto)
- Обработка ошибок, повторная обработка, DLQ Kafka Streams:
   - Минимум одна трасформация 
   - Минимум одна агрегация
   - Минимум одно оконное вычисление
- Запись результатов в отдельный топик Kafka Connect:
- Подключение к БД на чтение/запись
- Посмотреть avro/protobuf