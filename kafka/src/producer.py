import json
import uuid
import time
import random
from datetime import datetime
from confluent_kafka import Producer

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "orders-events"

# Типы событий
EVENT_TYPES = ["OrderCreated", "OrderPaid", "OrderCancelled"]

def get_producer_config():
    """Конфигурация продюсера"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'order-producer'
    }

def create_event(event_type):
    """Создание события по единому формату"""
    return {
        "eventId": str(uuid.uuid4()),
        "eventType": event_type,
        "entityId": f"order-{random.randint(1000, 9999)}",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "order-service",
        "version": "1.0",
        "payload": {
            "orderId": f"order-{random.randint(1000, 9999)}",
            "customerId": f"customer-{random.randint(1, 100)}",
            "amount": round(random.uniform(100, 5000), 2),
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
    
    counter = 0
    while True:
        # Выбираем случайный тип события
        event_type = random.choice(EVENT_TYPES)
        
        # Создаем событие
        event = create_event(event_type)
        
        # Ключ сообщения - entityId для партиционирования
        key = event["entityId"].encode('utf-8')
        
        # Отправляем сообщение
        producer.produce(
            topic=TOPIC_NAME,
            key=key,
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_callback
        )
        
        counter += 1
        print(f"[{counter}] Отправлено: {event_type} для {event['entityId']}")
        
        # Флеш и пауза
        producer.flush()
        time.sleep(2)

if __name__ == "__main__":
    main()