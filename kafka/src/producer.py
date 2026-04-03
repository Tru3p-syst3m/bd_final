import json
import uuid
import time
import random
from datetime import datetime
from confluent_kafka import Producer
import fastavro
from fastavro.schema import load_schema

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "orders-events"
SCHEMA_FILE = "/app/kafka/schema/event.avsc"  # Путь внутри Docker контейнера

# Типы событий
EVENT_TYPES = ["OrderCreated", "OrderPaid", "OrderCancelled"]

# Загружаем схему AVRO при старте
print(f"Загрузка схемы AVRO из {SCHEMA_FILE}...")
parsed_schema = fastavro.parse_schema(load_schema(SCHEMA_FILE))
print("Схема успешно загружена и готова к использованию.")

def get_producer_config():
    """Конфигурация продюсера"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'order-producer'
    }

def create_event(event_type):
    """Создание события по единому формату (соответствует схеме AVRO)"""
    order_id = f"order-{random.randint(1000, 9999)}"
    customer_id = f"customer-{random.randint(1, 100)}"
    amount = round(random.uniform(100, 5000), 2)
    
    return {
        "eventId": str(uuid.uuid4()),
        "eventType": event_type,
        "entityId": order_id,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "order-service",
        "version": "1.0",
        "payload": {
            "orderId": order_id,
            "customerId": customer_id,
            "amount": amount,
            "currency": "RUB",
            "status": get_status_by_type(event_type)
        }
    }

def get_status_by_type(event_type):
    """Статус заказа в зависимости от типа события"""
    statuses = {
        "OrderCreated": "created",
        "OrderPaid": "paid",
        "OrderCancelled": "cancelled"
    }
    return statuses.get(event_type, "unknown")

def serialize_to_avro(event):
    """Сериализация события в бинарный формат Avro"""
    return fastavro.schemaless_writer.serialize(parsed_schema, event)

def delivery_callback(err, msg):
    """Коллбек подтверждения доставки"""
    if err is not None:
        print(f"Ошибка доставки: {err}")
    else:
        print(f"Сообщение доставлено: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def main():
    """Основной цикл продюсера"""
    producer = Producer(get_producer_config())
    
    print(f"Продюсер запущен. Топик: {TOPIC_NAME}")
    print("Формат сообщений: AVRO (бинарный)")
    
    counter = 0
    while True:
        # Выбираем случайный тип события
        event_type = random.choice(EVENT_TYPES)
        
        # Создаем событие
        event = create_event(event_type)
        
        # Сериализуем в AVRO
        avro_message = serialize_to_avro(event)
        
        # Ключ сообщения - entityId для партиционирования
        key = event["entityId"].encode('utf-8')
        
        # Отправляем сообщение в бинарном формате AVRO
        producer.produce(
            topic=TOPIC_NAME,
            key=key,
            value=avro_message,
            callback=delivery_callback
        )
        
        counter += 1
        print(f"[{counter}] Отправлено: {event_type} для {event['entityId']} (AVRO)")
        
        # Флеш и пауза
        producer.flush()
        time.sleep(2)

if __name__ == "__main__":
    main()