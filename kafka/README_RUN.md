# Интернет-магазин: Kafka Event Streaming

Простая демонстрация работы с Kafka для учебного проекта.

## Что делает система

1. **Producer** - генерирует события заказов (OrderCreated, OrderPaid, OrderCancelled)
2. **Consumer** - обрабатывает события (автокоммит и ручной коммит)
3. **Stream App** - трансформирует, агрегирует и считает окна
4. **Kafka Connect** - синхронизирует данные с PostgreSQL
5. **PostgreSQL** - хранит заказы

## Быстрый старт

### 1. Запуск

запуск идет по профилям: 
 - профиль **kafka** это все сервисы с кафкой(нужно время чтобы инит отработал)

```bash
docker compose --profile kafka up --build -d
```

 - профиль **workers** это все сервисы с воркерам(потребители и продюсер)

```bash
docker compose --profile workers up --build -d
```

### 2. Проверка что всё работает

Открой в браузере:
- **Kafka UI**: http://localhost:8080 - посмотреть топики и сообщения

### 3. Логи сервисов

```bash
# Producer (генерация событий)
docker logs -f kafka-producer

# Consumer (обработка событий)
docker logs -f kafka-consumer-1

docker logs -f kafka-consumer-2

# Stream App (трансформация и агрегация)
docker logs -f kafka-stream-app

# Init скрипт (создание топиков)
docker logs -f kafka-init
```
### 5. Остановка

```bash
docker-compose stop <имя>
```

Для полной очистки (удаление всех данных):
```bash
docker-compose down -v
```