import json
import time
import os
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Загружаем схему из файла .avsc
from schema_loader import get_order_event_schema

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_NAME = "orders-events"
GROUP_ID = os.getenv("CONSUMER_GROUP", "order-consumer-auto")

# Инициализация клиента Schema Registry и десериализатора
print(f"Подключение к Schema Registry: {SCHEMA_REGISTRY_URL}...")
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# Загружаем схему для десериализации
AVRO_SCHEMA_STR = get_order_event_schema()
avro_deserializer = AvroDeserializer(schema_registry_client, AVRO_SCHEMA_STR)
print("Десериализатор AVRO готов.")

def get_consumer_config():
    """Конфигурация консьюмера с автокоммитом"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000
    }

def deserialize_from_avro(binary_message):
    """Десериализация сообщения из бинарного формата Avro через Schema Registry"""
    return avro_deserializer(
        binary_message, 
        SerializationContext(TOPIC_NAME, MessageField.VALUE)
    )

def process_event(event):
    """Обработка события"""
    print(f"  [AUTO] Обработано событие: {event['eventType']}")
    print(f"         Заказ: {event['entityId']}, Сумма: {event['payload'].get('amount', 'N/A')}")
    
    # Имитация обработки
    if event['eventType'] == 'OrderCancelled':
        print("         -> Заказ отменён")
    elif event['eventType'] == 'OrderPaid':
        print("         -> Заказ оплачен")
    else:
        print("         -> Заказ создан")

def main():
    """Основной цикл консьюмера с автокоммитом"""
    consumer = Consumer(get_consumer_config())
    consumer.subscribe([TOPIC_NAME])
    
    print(f"Консьюмер запущен (автокоммит). Группа: {GROUP_ID}")
    print("Формат сообщений: AVRO (бинарный)")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"Ошибка: {msg.error()}")
                continue
            
            # Десериализуем сообщение из AVRO
            try:
                event = deserialize_from_avro(msg.value())
                print(f"\n[AUTO] Получено: {event['eventId']}")
                process_event(event)
            except Exception as e:
                print(f"Ошибка десериализации AVRO: {e}")
            
            # Автокоммит происходит автоматически
            time.sleep(0.5)
    
    except KeyboardInterrupt:
        print("\nОстановка консьюмера...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()