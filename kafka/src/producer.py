import json
import uuid
import time
import random
import os
import sqlite3
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from schema_loader import get_order_event_schema

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_NAME = os.getenv("TOPIC_NAME", "orders-events")
AVRO_SCHEMA_STR = get_order_event_schema()

# Инициализация Schema Registry и сериализатора
print(f"Подключение к Schema Registry: {SCHEMA_REGISTRY_URL}...")
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_serializer = AvroSerializer(schema_registry_client, AVRO_SCHEMA_STR)
print("Сериализатор AVRO готов.")

# Инициализация SQLite
DB_PATH = os.getenv("DB_PATH", "orders.db")
sqlite_connection = sqlite3.connect(DB_PATH)
print(f"SQLite подключен: {DB_PATH}")

# Типы событий
EVENT_TYPES = ["OrderCreated", "OrderPaid", "OrderCancelled"]
order_id_increment = 0

def init_db(connection):
    """Инициализация таблицы заказов в SQLite"""
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            status TEXT NOT NULL
        )
    """)
    connection.commit()
    print("Таблица orders инициализирована.")

def write_order_to_db(connection, order_id, customer_id, status):
    """Запись заказа в таблицу SQLite"""
    cursor = connection.cursor()
    try:
        cursor.execute(
            "INSERT OR REPLACE INTO orders (order_id, customer_id, status) VALUES (?, ?, ?)",
            (order_id, customer_id, status)
        )
        connection.commit()
        print(f"Заказ сохранен: {order_id} -> {status}")
    except sqlite3.Error as e:
        print(f"Ошибка записи в БД: {e}")

def get_producer_config():
    """Конфигурация продюсера"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'order-producer'
    }

def get_customer_orders_with_status(connection, customer_id, status):
    """Получить все заказы покупателя с указанным статусом"""
    cursor = connection.cursor()
    cursor.execute(
        "SELECT order_id FROM orders WHERE customer_id = ? AND status = ?",
        (customer_id, status)
    )
    return [row[0] for row in cursor.fetchall()]

def create_event(connection):
    """Создание события по единому формату (соответствует схеме AVRO)"""
    customer_id = f"customer-{random.randint(1, 10)}"

    created_orders = get_customer_orders_with_status(connection, customer_id, "created")
    choice = random.randint(0, 10)
    if created_orders & choice > 3:
        order_id = random.choice(created_orders)
        event_type = random.choice(["OrderPaid", "OrderCancelled"])
        status = get_status_by_type(event_type)
    else:
        event_type = "OrderCreated"
        order_id = f"order-{random.randint(1000, 9999)}"
        status = get_status_by_type(event_type)

    amount = round(random.uniform(100, 5000), 2) if event_type == "OrderPaid" else 0

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
            "status": status
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
    """Сериализация события в бинарный формат Avro через Schema Registry"""
    return avro_serializer(
        event,
        SerializationContext(TOPIC_NAME, MessageField.VALUE)
    )

def delivery_callback(err, msg):
    """Коллбек подтверждения доставки"""
    if err is not None:
        print(f"Ошибка доставки: {err}")
    else:
        print(f"Сообщение доставлено: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def main():
    """Основной цикл продюсера"""
    init_db(sqlite_connection)
    producer = Producer(get_producer_config())
    
    print(f"Продюсер запущен. Топик: {TOPIC_NAME}")
    print("Формат сообщений: AVRO (бинарный)")
    
    counter = 0
    while True:
        # Создаём событие с проверкой БД
        event = create_event(sqlite_connection)

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

        # Сохраняем заказ в SQLite
        payload = event["payload"]
        write_order_to_db(
            sqlite_connection,
            payload["orderId"],
            payload["customerId"],
            payload["status"]
        )

        counter += 1
        print(f"[{counter}] Отправлено: {event['eventType']} для {event['entityId']} (AVRO)")
        
        # Флеш и пауза
        producer.flush()
        time.sleep(2)

if __name__ == "__main__":
    main()