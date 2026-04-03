# Интернет-магазин: Kafka Event Streaming

Простая демонстрация работы с Kafka для учебного проекта.

## Предварительная настройка (ВАЖНО!)

### Шаг 0: Скачать JDBC коннектор

Перед запуском нужно скачать JDBC коннектор вручную, чтобы не ждать его загрузку при сборке Docker.

**Команда для скачивания:**
```bash
cd kafka
mkdir -p jdbc-connector
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-jdbc/versions/10.7.4/confluentinc-kafka-connect-jdbc-10.7.4.zip -O jdbc-connector/kafka-connect-jdbc.zip
```

Или через curl:
```bash
cd kafka
mkdir -p jdbc-connector
curl -L https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-jdbc/versions/10.7.4/confluentinc-kafka-connect-jdbc-10.7.4.zip -o jdbc-connector/kafka-connect-jdbc.zip
```

**Распаковать архив:**
```bash
cd jdbc-connector
unzip kafka-connect-jdbc.zip
```

После распаковки структура должна быть такой:
```
kafka/
├── jdbc-connector/
│   └── confluentinc-kafka-connect-jdbc-10.7.4/
│       ├── lib/
│       │   └── kafka-connect-jdbc-10.7.4.jar
│       └── ...
```

Коннектор будет автоматически подключен через volume в docker-compose.yml.

---

## Что делает система

1. **Producer** - генерирует события заказов (OrderCreated, OrderPaid, OrderCancelled)
2. **Consumer** - обрабатывает события (автокоммит и ручной коммит)
3. **Stream App** - трансформирует, агрегирует и считает окна
4. **Kafka Connect** - синхронизирует данные с PostgreSQL
5. **PostgreSQL** - хранит заказы

## Структура проекта

```
kafka/
├── docker-compose.yml      # Все сервисы (Kafka, Zookeeper, Postgres и т.д.)
├── init-kafka.sh          # Скрипт инициализации (топики + коннекторы)
├── bd/
│   └── init.sql           # Схема БД
├── schema/                # AVRO схемы
│   ├── event.avsc
│   ├── transformed_event.avsc
│   ├── aggregated_event.avsc
│   └── windowed_event.avsc
├── config/                # Конфиги коннекторов
│   ├── sink_connector.json
│   └── source_connector.json
├── src/                   # Python код
│   ├── producer.py
│   ├── consumer_auto.py
│   ├── consumer_manual.py
│   ├── stream_app.py
│   └── schema_loader.py
├── kafka-producer/        # Dockerfile для producer
└── kafka-consumer/        # Dockerfile для consumer
```

## Быстрый старт

### 1. Запуск всех сервисов

```bash
cd kafka
docker-compose up --build -d
```

Ждем 1-2 минуты пока все поднимется.

### 2. Проверка что всё работает

Открой в браузере:
- **Kafka UI**: http://localhost:8080 - посмотреть топики и сообщения
- **Kafka Connect API**: http://localhost:8083/connectors - список коннекторов

### 3. Логи сервисов

```bash
# Producer (генерация событий)
docker logs -f kafka-producer

# Consumer (обработка событий)
docker logs -f kafka-consumer

# Stream App (трансформация и агрегация)
docker logs -f kafka-stream-app

# Init скрипт (создание топиков)
docker logs -f kafka-init

# Sink коннектор (запись в БД)
curl http://localhost:8083/connectors/postgres-sink-connector/status | python3 -m json.tool
```

### 4. Проверка данных в БД

```bash
docker exec -it postgres psql -U postgres -d shopdb -c "SELECT * FROM orders LIMIT 10;"
```

### 5. Остановка

```bash
docker-compose down
```

Для полной очистки (удаление всех данных):
```bash
docker-compose down -v
```

## Топики

| Топик | Описание |
|-------|----------|
| `orders-events` | Основные события заказов |
| `orders-transformed` | Трансформированные события (добавлено processedAt) |
| `customer-order-totals` | Агрегация: сумма заказов по клиентам |
| `orders-windowed-count` | Оконное вычисление: кол-во заказов за 5 мин |
| `shop-db-orders` | Данные из БД (source connector) |
| `orders-dlq` | Мертвые письма (ошибки) |

## Требования из задания

✅ **Producer:**
- 3 типа событий: OrderCreated, OrderPaid, OrderCancelled
- Единый формат сообщений (AVRO схема)
- Ключ сообщения: entityId (orderId)
- Метаданные: eventId, timestamp, source, version

✅ **Consumer:**
- 2 consumer group: order-consumer-auto, order-consumer-manual
- Auto commit и manual commit
- Обработка ошибок с логированием

✅ **Kafka Streams:**
- Трансформация: добавление processedAt
- Агрегация: сумма заказов по клиенту
- Оконное вычисление: 5 минутное окно

✅ **Kafka Connect:**
- Sink: Kafka → PostgreSQL
- Source: PostgreSQL → Kafka
- AVRO сериализация через Schema Registry

## Как это работает (просто)

1. **kafka-init** запускается первым, создает топики и регистрирует коннекторы
2. **producer** начинает слать события в топик `orders-events`
3. **consumer** читает события и логирует обработку
4. **stream-app** читает события, трансформирует их и пишет в другие топики
5. **sink-connector** автоматически пишет все события из `orders-events` в PostgreSQL
6. **source-connector** читает изменения из БД и шлет в топик `shop-db-orders`

## Troubleshooting

### Producer не подключается
```bash
docker logs kafka-producer
# Проверь что kafka и schema-registry запущены
docker ps
```

### Коннектор упал
```bash
# Пересоздать коннектор
curl -X DELETE http://localhost:8083/connectors/postgres-sink-connector
curl -X POST -H "Content-Type: application/json" \
  --data @config/sink_connector.json \
  http://localhost:8083/connectors
```

### Нет данных в БД
```bash
# Проверить статус sink коннектора
curl http://localhost:8083/connectors/postgres-sink-connector/status

# Посмотреть логи connect
docker logs kafka-connect
```

## Для преподавателя

Всё работает через docker-compose. Никаких дополнительных установок не требуется.
Код максимально простой и понятный, без лишних усложнений.

Основные файлы для проверки:
- `src/producer.py` - производитель событий
- `src/consumer_auto.py` - потребитель с автокоммитом
- `src/stream_app.py` - обработка потока (трансформация, агрегация, окна)
- `config/sink_connector.json` - конфиг записи в БД
- `schema/event.avsc` - схема события
