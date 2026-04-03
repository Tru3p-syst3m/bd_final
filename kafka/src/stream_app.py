import faust
from datetime import datetime

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_INPUT = "orders-events"
TOPIC_TRANSFORMED = "orders-transformed"
TOPIC_AGGREGATED = "orders-by-customer"
TOPIC_WINDOWED = "orders-windowed"

# Создаем приложение Faust
app = faust.App(
    'order-stream-app',
    broker=f'kafka://{KAFKA_BOOTSTRAP_SERVERS}',
    store='memory://'
)

# Модели данных
class OrderEvent(faust.Record):
    """Модель входного события заказа"""
    eventId: str
    eventType: str
    entityId: str
    timestamp: str
    source: str
    version: str
    payload: dict

class TransformedEvent(faust.Record):
    """Модель трансформированного события"""
    eventId: str
    eventType: str
    entityId: str
    timestamp: str
    processedAt: str
    source: str
    version: str
    payload: dict

# Таблицы для агрегации и окон
customer_totals = app.Table('customer-totals', default=float)
windowed_orders = app.Table('windowed-orders', default=int).tumbling(300, expires=305)

def get_transformed_event(order, processed_at):
    """Создание трансформированного события"""
    return TransformedEvent(
        eventId=order.eventId,
        eventType=order.eventType,
        entityId=order.entityId,
        timestamp=order.timestamp,
        processedAt=processed_at,
        source=order.source,
        version=order.version,
        payload=order.payload
    )

def send_aggregated_result(customer_id, total):
    """Отправка результата агрегации"""
    return {
        'customerId': customer_id,
        'totalAmount': total,
        'currency': 'RUB'
    }

def send_windowed_result(count):
    """Отправка результата оконного вычисления"""
    return {
        'window': '5min',
        'count': count,
        'timestamp': datetime.utcnow().isoformat()
    }

@app.agent(value_type=OrderEvent)
async def transform_orders(orders):
    """Трансформация событий - добавление processedAt"""
    async for order in orders:
        processed_at = datetime.utcnow().isoformat()
        transformed = get_transformed_event(order, processed_at)
        
        await app.send(TOPIC_TRANSFORMED, value=transformed)
        print(f"[TRANSFORM] {order.eventId} -> processedAt={processed_at}")
        yield transformed

@app.agent(value_type=OrderEvent)
async def aggregate_by_customer(orders):
    """Агрегация суммы заказов по клиентам"""
    async for order in orders.group_by(OrderEvent.entityId):
        customer_id = order.payload.get('customerId', 'unknown')
        amount = order.payload.get('amount', 0)
        
        if order.eventType == 'OrderPaid':
            customer_totals[customer_id] += amount
            total = customer_totals[customer_id]
            print(f"[AGGREGATE] {customer_id}: {total:.2f} RUB")
            
            result = send_aggregated_result(customer_id, total)
            await app.send(TOPIC_AGGREGATED, key=customer_id, value=result)

@app.agent(value_type=OrderEvent)
async def windowed_count(orders):
    """Оконное вычисление - количество заказов за 5 минут"""
    async for order in orders:
        windowed_orders['count'] += 1
        count = windowed_orders['count']
        
        print(f"[WINDOW] За 5 мин: {count} заказов")
        
        result = send_windowed_result(count)
        await app.send(TOPIC_WINDOWED, key='order_count', value=result)

@app.task
async def process_orders():
    """Основной цикл обработки событий"""
    consumer = app.consumer(topic=TOPIC_INPUT)
    async for event in consumer:
        try:
            data = event.value
            order = OrderEvent(**data)
            
            await transform_orders.send(value=order)
            await aggregate_by_customer.send(value=order)
            await windowed_count.send(value=order)
            
        except Exception as e:
            print(f"[ERROR] Ошибка: {e}")

if __name__ == '__main__':
    app.main()
