import json
import time
from confluent_kafka import Consumer, KafkaException, TopicPartition

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "orders-events"
GROUP_ID = "order-consumer-manual"
DLQ_TOPIC = "orders-dlq"  # Топик для ошибочных сообщений

def get_consumer_config():
    """Конфигурация консьюмера с ручным коммитом"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # Отключаем автокоммит
    }

def process_event(event):
    """Обработка события с возможной ошибкой"""
    print(f"  [MANUAL] Обработано событие: {event['eventType']}")
    print(f"           Заказ: {event['entityId']}, Сумма: {event['payload'].get('amount', 'N/A')}")
    
    # Имитация ошибки для отменённых заказов (для демонстрации DLQ)
    if event['eventType'] == 'OrderCancelled':
        raise Exception("Имитация ошибки обработки отменённого заказа")
    
    if event['eventType'] == 'OrderPaid':
        print("           -> Заказ оплачен, записываем в БД")
    else:
        print("           -> Заказ создан")
    
    return True

def send_to_dlq(producer, msg, error):
    """Отправка сообщения в DLQ (Dead Letter Queue)"""
    print(f"           -> Отправка в DLQ: {error}")
    producer.produce(
        topic=DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers=[('error', str(error).encode())]
    )
    producer.flush()

def main():
    """Основной цикл консьюмера с ручным коммитом"""
    from confluent_kafka import Producer
    
    consumer = Consumer(get_consumer_config())
    consumer.subscribe([TOPIC_NAME])
    
    dlq_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    
    print(f"Консьюмер запущен (ручной коммит). Группа: {GROUP_ID}")
    print(f"DLQ топик: {DLQ_TOPIC}")
    
    max_retries = 3
    
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
                print(f"\n[MANUAL] Получено: {event['eventId']} (попытка 1)")
                
                retry_count = 0
                success = False
                
                while retry_count < max_retries and not success:
                    try:
                        success = process_event(event)
                    except Exception as e:
                        retry_count += 1
                        print(f"           Ошибка обработки: {e}")
                        
                        if retry_count < max_retries:
                            print(f"           Повторная попытка {retry_count}/{max_retries}...")
                            time.sleep(1)
                        else:
                            print(f"           Превышено количество попыток, отправка в DLQ")
                            send_to_dlq(dlq_producer, msg, e)
                            # Коммитим даже ошибочное, чтобы не зациклиться
                            consumer.commit(msg)
                            break
                
                if success:
                    # Ручной коммит после успешной обработки
                    consumer.commit(msg)
                    print("           -> Коммит оффсета выполнен")
                    
            except json.JSONDecodeError as e:
                print(f"Ошибка парсинга JSON: {e}")
                consumer.commit(msg)  # Коммитим битое сообщение чтобы не зациклиться
            
            time.sleep(0.5)
    
    except KeyboardInterrupt:
        print("\nОстановка консьюмера...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()