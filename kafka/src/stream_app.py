"""
Простое приложение Kafka Streams для обработки событий заказов.
Использует confluent-kafka вместо Faust для простоты.

Функционал:
1. Трансформация - добавление processedAt
2. Агрегация - сумма заказов по клиентам
3. Оконное вычисление - количество заказов за 5 минут
"""

import json
import time
from datetime import datetime, timedelta
from collections import defaultdict
from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Загружаем схемы из файлов .avsc
from schema_loader import (
    get_order_event_schema,
    get_transformed_event_schema,
    get_aggregated_event_schema,
    get_windowed_event_schema
)
customer_window_data = defaultdict(list)

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# Топики
TOPIC_INPUT = "orders-events"
TOPIC_TRANSFORMED = "orders-transformed"
TOPIC_AGGREGATED = "customer-order-totals"
TOPIC_WINDOWED = "orders-windowed-count"

# Загружаем все схемы из файлов
INPUT_SCHEMA_STR = get_order_event_schema()
TRANSFORMED_SCHEMA = get_transformed_event_schema()
AGGREGATED_SCHEMA = get_aggregated_event_schema()
WINDOWED_SCHEMA = get_windowed_event_schema()

print(f"Подключение к Schema Registry: {SCHEMA_REGISTRY_URL}...")
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# Десериализатор для входных сообщений (использует схему из Schema Registry)
avro_deserializer = AvroDeserializer(schema_registry_client, INPUT_SCHEMA_STR)

# Сериализаторы для выходных топиков
transformed_serializer = AvroSerializer(schema_registry_client, TRANSFORMED_SCHEMA)
aggregated_serializer = AvroSerializer(schema_registry_client, AGGREGATED_SCHEMA)
windowed_serializer = AvroSerializer(schema_registry_client, WINDOWED_SCHEMA)

print("Сериализаторы и десериализаторы готовы.")

# Хранилище состояния для агрегации
customer_totals = defaultdict(float)

# Хранилище для оконных вычислений (5 минут = 300 секунд)
WINDOW_SIZE_SECONDS = 300
window_events = []  # Список (timestamp, count)

def get_consumer_config():
    """Конфигурация консьюмера"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'stream-app-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }

def get_producer_config():
    """Конфигурация продюсера"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
    }

def deserialize_event(binary_message):
    """Десериализация входного события"""
    return avro_deserializer(
        binary_message,
        SerializationContext(TOPIC_INPUT, MessageField.VALUE)
    )

def transform_event(event):
    """Трансформация события - добавление processedAt"""
    processed_at = datetime.utcnow().isoformat()
    transformed = {
        "eventId": event["eventId"],
        "eventType": event["eventType"],
        "entityId": event["entityId"],
        "timestamp": event["timestamp"],
        "processedAt": processed_at,
        "source": event["source"],
        "version": event["version"],
        "payload": {
            "orderId": event["payload"]["orderId"],
            "customerId": event["payload"]["customerId"],
            "amount": float(event["payload"]["amount"]),
            "currency": event["payload"]["currency"],
            "status": event["payload"]["status"]
        }
    }
    return transformed, processed_at

def aggregate_event(event):
    """Агрегация суммы заказов по клиенту"""
    customer_id = event["payload"].get("customerId", "unknown")
    # тут дожно быть обращение к базе данных с поиском этого клиена
    # но тут стоит заглушка
    amount = float(event["payload"].get("amount", 0))
    
    if event["eventType"] == "OrderPaid":
        customer_totals[customer_id] += amount
        total = customer_totals[customer_id]
        
        result = {
            "customerId": customer_id,
            "totalAmount": total,
            "currency": "RUB",
            "timestamp": datetime.utcnow().isoformat()
        }
        return result, total
    return None, 0

def windowed_count(event):
    """Оконное вычисление - количество заказов за 5 минут"""
    now = datetime.utcnow()
    window_start = now - timedelta(seconds=60)
    
    cust_id = event["payload"]["customerId"]
    amount = float(event["payload"]["amount"])
    
    # Фиксируем только оплаты
    customer_window_data[cust_id].append((now, amount))
        
    # Удаляем записи старше 1 минуты
    customer_window_data[cust_id] = [
        (ts, amt) for ts, amt in customer_window_data[cust_id] 
        if ts > window_start
    ]
    count = sum(amt for _, amt in customer_window_data[cust_id])
    
    result = {
        "customerId": cust_id,
        "window": "1min",
        "count": count,
        "timestamp": now.isoformat()
    }
    return result, count

def main():
    """Основной цикл обработки"""
    consumer = Consumer(get_consumer_config())
    producer = Producer(get_producer_config())
    
    consumer.subscribe([TOPIC_INPUT])
    
    print(f"\n=== Stream App запущен ===")
    print(f"Входной топик: {TOPIC_INPUT}")
    print(f"Топик трансформированных: {TOPIC_TRANSFORMED}")
    print(f"Топик агрегированных: {TOPIC_AGGREGATED}")
    print(f"Топик оконных: {TOPIC_WINDOWED}")
    print("=" * 40)
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"Ошибка: {msg.error()}")
                continue
            
            # Десериализация события
            try:
                event = deserialize_event(msg.value())
                event_id = event.get("eventId", "unknown")
                event_type = event.get("eventType", "unknown")
                
                print(f"\n[STREAM] Получено событие: {event_id} ({event_type})")
                
                # 1. Трансформация
                transformed, processed_at = transform_event(event)
                producer.produce(
                    topic=TOPIC_TRANSFORMED,
                    key=transformed["eventId"].encode('utf-8'),
                    value=transformed_serializer(transformed, SerializationContext(TOPIC_TRANSFORMED, MessageField.VALUE)),
                    callback=lambda err, msg: print(f"  [TRANSFORM] Отправлено в {msg.topic()}") if not err else print(f"  [TRANSFORM] Ошибка: {err}")
                )
                print(f"  [TRANSFORM] Добавлено processedAt={processed_at}")
                
                # 2. Агрегация
                agg_result, total = aggregate_event(event)
                if agg_result:
                    producer.produce(
                        topic=TOPIC_AGGREGATED,
                        key=agg_result["customerId"].encode('utf-8'),
                        value=aggregated_serializer(agg_result, SerializationContext(TOPIC_AGGREGATED, MessageField.VALUE)),
                        callback=lambda err, msg: print(f"  [AGGREGATE] Отправлено в {msg.topic()}") if not err else print(f"  [AGGREGATE] Ошибка: {err}")
                    )
                    print(f"  [AGGREGATE] {agg_result['customerId']}: {total:.2f} RUB")
                
                # 3. Оконное вычисление
                if event.get("eventType") == "OrderPaid":
                    window_result, count = windowed_count(event)
                    producer.produce(
                        topic=TOPIC_WINDOWED,
                        key=window_result["customerId"].encode('utf-8'), 
                        value=windowed_serializer(window_result, SerializationContext(TOPIC_WINDOWED, MessageField.VALUE)),
                        callback=lambda err, msg: print(f"  [WINDOW] Отправлено в {msg.topic()}") if not err else print(f"  [WINDOW] Ошибка: {err}")
                    )
                    print(f"  [WINDOW] За 5 мин: {count} заказов")
                
                producer.flush()
                
            except Exception as e:
                print(f"Ошибка обработки: {e}")
                continue
    
    except KeyboardInterrupt:
        print("\nОстановка Stream App...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
