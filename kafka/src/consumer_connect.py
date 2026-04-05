import json
import time
import os
from typing import List, Dict, Any
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from clickhouse_driver import Client
from schema_loader import load_schema

# ============================================
# Конфигурация
# ============================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "9000")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# 3 топика от Stream App
TOPICS_CONFIG = {
    "orders-transformed": {
        "schema_file": "transformed_event",
        "table": "orders_raw",
    },
    "customer-order-totals": {
        "schema_file": "aggregated_event",
        "table": "customer_totals",
    },
    "orders-windowed-count": {
        "schema_file": "windowed_event",
        "table": "order_windows",
    }
}

DLQ_TOPIC = "orders-dlq"
BATCH_SIZE = 50  # Размер пачки для вставки в ClickHouse
BASE_GROUP_ID = os.getenv("CONSUMER_GROUP", "clickhouse-consumer")

# ============================================
# Инициализация клиентов
# ============================================
print(f"Подключение к Schema Registry: {SCHEMA_REGISTRY_URL}...")
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

deserializers = {}
for topic, config in TOPICS_CONFIG.items():
    schema_str = load_schema(config["schema_file"])
    deserializers[topic] = AvroDeserializer(schema_registry_client, schema_str)
    print(f"  Десериализатор для {topic} готов")

print(f"Подключение к ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
ch_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    database=CLICKHOUSE_DB,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD
)
print("ClickHouse клиент готов.")

# ============================================
# Функции работы с БД
# ============================================
def insert_to_orders_raw(events: List[Dict[str, Any]]):
    """Вставка детальных событий в orders_raw"""
    if not events:
        return
    
    query = """
    INSERT INTO orders_raw 
    (event_id, event_type, entity_id, timestamp, processed_at, source, version,
     order_id, customer_id, amount, currency, status)
    VALUES
    """
    
    values = []
    for e in events:
        ts = e.get('timestamp', '')
        processed = e.get('processedAt', '')
        payload = e.get('payload', {})
        
        values.append((
            e.get('eventId', ''),
            e.get('eventType', ''),
            e.get('entityId', ''),
            ts.replace('T', ' ').replace('Z', '') if ts else None,
            processed.replace('T', ' ').replace('Z', '') if processed else None,
            e.get('source', ''),
            e.get('version', ''),
            payload.get('orderId', ''),
            payload.get('customerId', ''),
            float(payload.get('amount', 0)),
            payload.get('currency', ''),
            payload.get('status', ''),
        ))
    
    ch_client.execute(query, values)
    print(f"  [CH] Вставлено {len(values)} записей в orders_raw")

def insert_to_customer_totals(events: List[Dict[str, Any]]):
    """Вставка агрегатов по клиентам в customer_totals"""
    if not events:
        return
    
    query = """
    INSERT INTO customer_totals 
    (customer_id, total_amount, currency, timestamp)
    VALUES
    """
    
    values = []
    for e in events:
        ts = e.get('timestamp', '')
        values.append((
            e.get('customerId', ''),
            float(e.get('totalAmount', 0)),
            e.get('currency', ''),
            ts.replace('T', ' ').replace('Z', '') if ts else None,
        ))
    
    ch_client.execute(query, values)
    print(f"  [CH] Вставлено {len(values)} записей в customer_totals")

def insert_to_order_windows(events: List[Dict[str, Any]]):
    """Вставка оконных агрегатов в order_windows"""
    if not events:
        return
    
    query = """
    INSERT INTO order_windows 
    (window, count, timestamp)
    VALUES
    """
    
    values = []
    for e in events:
        ts = e.get('timestamp', '')
        values.append((
            e.get('window', ''),
            int(e.get('count', 0)),
            ts.replace('T', ' ').replace('Z', '') if ts else None,
        ))
    
    ch_client.execute(query, values)
    print(f"  [CH] Вставлено {len(values)} записей в order_windows")

# ============================================
# Логика потребителя
# ============================================
def get_consumer_config(group_id: str):
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

def deserialize_from_avro(binary_message, topic: str):
    """Десериализация с использованием правильного десериализатора для топика"""
    deserializer = deserializers.get(topic)
    if not deserializer:
        raise ValueError(f"Нет десериализатора для топика {topic}")
    
    return deserializer(
        binary_message,
        SerializationContext(topic, MessageField.VALUE)
    )

def send_to_dlq(producer, msg, error):
    """Отправка сообщения в Dead Letter Queue"""
    print(f"           -> Отправка в DLQ: {error}")
    producer.produce(
        topic=DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers=[('error', str(error).encode())]
    )
    producer.flush()

def get_table_for_topic(topic: str) -> str:
    """Возвращает имя таблицы ClickHouse для топика"""
    return TOPICS_CONFIG.get(topic, {}).get("table")

def flush_batch(batch: Dict[str, List]):
    """Отправка накопленных пачек в ClickHouse"""
    try:
        if batch['orders_raw']:
            insert_to_orders_raw(batch['orders_raw'])
            batch['orders_raw'] = []
            
        if batch['customer_totals']:
            insert_to_customer_totals(batch['customer_totals'])
            batch['customer_totals'] = []
            
        if batch['order_windows']:
            insert_to_order_windows(batch['order_windows'])
            batch['order_windows'] = []
            
    except Exception as e:
        print(f"Ошибка записи в ClickHouse: {e}")
        raise

def main():
    # Один консьюмер может подписаться на несколько топиков
    # Но group.id должен быть один для всех (или разные если нужно независимое потребление)
    consumer = Consumer(get_consumer_config(BASE_GROUP_ID))
    consumer.subscribe(list(TOPICS_CONFIG.keys()))
    
    dlq_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    print(f"Консьюмер запущен. Группа: {BASE_GROUP_ID}")
    print(f"Топики: {list(TOPICS_CONFIG.keys())}")
    print(f"Цель: ClickHouse ({CLICKHOUSE_HOST})")
    
    batch = {
        'orders_raw': [],
        'customer_totals': [],
        'order_windows': []
    }
    messages_processed = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                # Таймаут - проверяем, не пора ли сбросить пачку
                if any(len(v) > 0 for v in batch.values()):
                    flush_batch(batch)
                continue
            
            if msg.error():
                print(f"Ошибка Kafka: {msg.error()}")
                continue
            
            topic = msg.topic()
            
            try:
                # Десериализуем с правильным десериализатором для этого топика
                event = deserialize_from_avro(msg.value(), topic)
                table = get_table_for_topic(topic)
                
                print(f"\n[CH] Топик: {topic} -> Таблица: {table}")
                print(f"       Событие: {event.get('eventId', event.get('customerId', event.get('window', 'N/A')))}")
                
                # Маршрутизация по таблице (на основе топика)
                if table == 'orders_raw':
                    batch['orders_raw'].append(event)
                elif table == 'customer_totals':
                    batch['customer_totals'].append(event)
                elif table == 'order_windows':
                    batch['order_windows'].append(event)
                
                messages_processed += 1
                
                # Если набрали пачку - пишем в БД
                total_batch_size = sum(len(v) for v in batch.values())
                if total_batch_size >= BATCH_SIZE:
                    flush_batch(batch)
                
                # Коммит оффсета
                consumer.commit(msg)
                
            except Exception as e:
                print(f"Ошибка обработки: {e}")
                send_to_dlq(dlq_producer, msg, e)
                consumer.commit(msg)
            
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nОстановка консьюмера...")
        # Сброс остатков пачки перед выходом
        if any(len(v) > 0 for v in batch.values()):
            flush_batch(batch)
    finally:
        consumer.close()
        ch_client.disconnect()

if __name__ == "__main__":
    main()