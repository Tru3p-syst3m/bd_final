import json
import time
from confluent_kafka import Consumer, KafkaException
import fastavro
from fastavro.schema import load_schema

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "orders-events"
GROUP_ID = "order-consumer-auto"
SCHEMA_FILE = "/app/kafka/schema/event.avsc"

# Загружаем схему AVRO при старте
print(f"Загрузка схемы AVRO из {SCHEMA_FILE}...")
parsed_schema = fastavro.parse_schema(load_schema(SCHEMA_FILE))
print("Схема успешно загружена.")

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
    """Десериализация сообщения из бинарного формата Avro"""
    return fastavro.schemaless_reader(parsed_schema, binary_message)

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