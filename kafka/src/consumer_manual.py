import json
import time
from confluent_kafka import Consumer, KafkaException, TopicPartition, Producer
import fastavro
from fastavro.schema import load_schema

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "orders-events"
GROUP_ID = "order-consumer-manual"
DLQ_TOPIC = "orders-dlq"  # Топик для ошибочных сообщений
SCHEMA_FILE = "/app/kafka/schema/event.avsc"

# Загружаем схему AVRO при старте
print(f"Загрузка схемы AVRO из {SCHEMA_FILE}...")
parsed_schema = fastavro.parse_schema(load_schema(SCHEMA_FILE))
print("Схема успешно загружена.")

def get_consumer_config():
    """Конфигурация консьюмера с ручным коммитом"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # Отключаем автокоммит
    }

def deserialize_from_avro(binary_message):
    """Десериализация сообщения из бинарного формата Avro"""
    return fastavro.schemaless_reader(parsed_schema, binary_message)

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
    consumer = Consumer(get_consumer_config())
    consumer.subscribe([TOPIC_NAME])
    
    dlq_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    
    print(f"Консьюмер запущен (ручной коммит). Группа: {GROUP_ID}")
    print(f"DLQ топик: {DLQ_TOPIC}")
    print("Формат сообщений: AVRO (бинарный)")
    
    max_retries = 3
    
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
                    
            except Exception as e:
                print(f"Ошибка десериализации AVRO: {e}")
                consumer.commit(msg)  # Коммитим битое сообщение чтобы не зациклиться
            
            time.sleep(0.5)
    
    except KeyboardInterrupt:
        print("\nОстановка консьюмера...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()