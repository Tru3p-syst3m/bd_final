#!/bin/bash
# Скрипт инициализации Kafka: создание топиков и регистрация коннекторов

set -e

KAFKA_BROKER="kafka:29092"
SCHEMA_REGISTRY="http://schema-registry:8081"
KAFKA_CONNECT="http://kafka-connect:8083"

echo "=== Инициализация Kafka ==="

# Ждем доступности Kafka
echo "Ожидаем доступности Kafka..."
while ! nc -z kafka 29092; do
  sleep 2
done
echo "Kafka доступна!"

# Ждем доступности Schema Registry
echo "Ожидаем доступности Schema Registry..."
while ! curl -s $SCHEMA_REGISTRY > /dev/null; do
  sleep 2
done
echo "Schema Registry доступен!"

# Ждем доступности Kafka Connect
echo "Ожидаем доступности Kafka Connect..."
while ! curl -s $KAFKA_CONNECT > /dev/null; do
  sleep 2
done
echo "Kafka Connect доступен!"

# Создаем топики
echo ""
echo "=== Создание топиков ==="

# Основной топик событий
kafka-topics --create --if-not-exists --topic orders-events \
  --bootstrap-server $KAFKA_BROKER \
  --partitions 3 --replication-factor 1
echo "✓ Топик orders-events создан"

# Топики для Stream App
kafka-topics --create --if-not-exists --topic orders-transformed \
  --bootstrap-server $KAFKA_BROKER \
  --partitions 3 --replication-factor 1
echo "✓ Топик orders-transformed создан"

kafka-topics --create --if-not-exists --topic customer-order-totals \
  --bootstrap-server $KAFKA_BROKER \
  --partitions 3 --replication-factor 1
echo "✓ Топик customer-order-totals создан"

kafka-topics --create --if-not-exists --topic orders-windowed-count \
  --bootstrap-server $KAFKA_BROKER \
  --partitions 1 --replication-factor 1
echo "✓ Топик orders-windowed-count создан"

# DLQ топик для ошибок
kafka-topics --create --if-not-exists --topic orders-dlq \
  --bootstrap-server $KAFKA_BROKER \
  --partitions 1 --replication-factor 1
echo "✓ Топик orders-dlq создан"

echo ""
echo "=== Регистрация схем в Schema Registry ==="

# Регистрируем схему для orders-events
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "'"$(cat /app/schema/event.avsc | tr -d '\n' | sed 's/"/\\"/g)"'"}' \
  $SCHEMA_REGISTRY/subjects/orders-events-value/versions
echo "✓ Схема orders-events зарегистрирована"

# Регистрируем схему для transformed
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "'"$(cat /app/schema/transformed_event.avsc | tr -d '\n' | sed 's/"/\\"/g)"'"}' \
  $SCHEMA_REGISTRY/subjects/orders-transformed-value/versions
echo "✓ Схема orders-transformed зарегистрирована"

# Регистрируем схему для aggregated
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "'"$(cat /app/schema/aggregated_event.avsc | tr -d '\n' | sed 's/"/\\"/g)"'"}' \
  $SCHEMA_REGISTRY/subjects/customer-order-totals-value/versions
echo "✓ Схема customer-order-totals зарегистрирована"

# Регистрируем схему для windowed
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "'"$(cat /app/schema/windowed_event.avsc | tr -d '\n' | sed 's/"/\\"/g)"'"}' \
  $SCHEMA_REGISTRY/subjects/orders-windowed-count-value/versions
echo "✓ Схема orders-windowed-count зарегистрирована"