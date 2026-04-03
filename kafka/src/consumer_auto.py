import json
import time
from confluent_kafka import Consumer, KafkaException

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "orders-events"
GROUP_ID = "order-consumer-auto"

def get_consumer_config():
    """Конфигурация консьюмера с автокоммитом"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000
    }

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
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"Ошибка: {msg.error()}")
                continue
            
            # Парсим сообщение
            try:
                event = json.loads(msg.value().decode('utf-8'))
                print(f"\n[AUTO] Получено: {event['eventId']}")
                process_event(event)
            except json.JSONDecodeError as e:
                print(f"Ошибка парсинга JSON: {e}")
            
            # Автокоммит происходит автоматически
            time.sleep(0.5)
    
    except KeyboardInterrupt:
        print("\nОстановка консьюмера...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()