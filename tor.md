

### Технологии

- Python 3.11 / 3.12  
- FastAPI + uvicorn 
- STOMP 1.2 over WebSocket
- PostgreSQL + asyncpg + SQLAlchemy 2.0 + alembic  
- prometheus-fastapi-instrumentator  
- pydantic v2, structlog, tenacity (для retry)

### Структура проекта 

```
project/
├── app/
│   ├── core/             # логика брокера 
│   ├── persistence/      # модели, репозитории, миграции
│   ├── protocol/         # stomp парсер + websocket handlers
│   ├── api/              # rest endpoints
│   ├── metrics/          # prometheus
│   └── main.py
├── sdk/                  # клиентская библиотека 
├── migrations/
├── tests/
├── docker-compose.yml
├── Dockerfile
└── docs/
    └── architecture.md
```

---

### Архитектура 


Слои архитектуры сверху вниз:

1. Клиенты
   - Publisher
   - Несколько Subscribers 
   - Queue consumers 

   Подключаются по:
   • WebSocket + STOMP 
   • REST API 

2. Вход / Протокол
   - WebSocket endpoint (/ws)
   - STOMP frame parser → превращает текст в объекты
   - REST контроллеры (/topics, /queues, /metrics)

3. Ядро брокера 
   - Topic — рассылка всем подписчикам
   - PersistentQueue FIFO + приоритеты
   - MessageRouter решает, топик это или очередь
   - TTL, Dead Letter Queue, redelivery

4. Хранение
   - PostgreSQL 
   - Таблицы: messages, subscriptions, dlq_messages
   - Все важные операции в транзакциях

5. Мониторинг
   - Prometheus exporter (/metrics)

---

Слои внутри монолита:

1. protocol → stomp фреймы ↔ python объекты  
2. core / domain → Topic, PersistentQueue, Message, Router  
3. storage → CRUD для сообщений и подписок  
4. presentation → websocket + rest + metrics

---

### Как выглядят сообщения 

```json
{
  "id": "uuid4 строка",
  "destination": "/topic/news.sport" | "/queue/payment.new",
  "body": "строка или base64 если бинарный",
  "headers": {
    "content-type": "application/json",
    "correlation-id": "abc123"
  },
  "priority": 3,          // 0 = самый важный, 9 = низкий
  "ttl_seconds": 3600,    // или null = никогда не умирать
  "published_at": "2026-03-22T14:35:12Z",
  "redelivery_count": 0
}
```

### Таблицы в постгресе

```sql
messages
├── id               uuid pk
├── destination      text not null          -- /topic/xxx или /queue/yyy
├── is_topic         bool not null
├── body             jsonb not null
├── headers          jsonb
├── priority         int default 5
├── ttl_seconds      int
├── published_at     timestamptz
├── expires_at       timestamptz          -- вычисляется
├── status           text                  -- pending / delivered / dead
└── redeliver_count  int default 0

subscriptions
├── id               serial pk
├── client_id        text not null         -- уникальный id клиента
├── destination      text not null
├── durable          bool default false
└── created_at       timestamptz

dlq (dead letter queue)
 - почти как messages + поле reason text
```

---

### План разработки 

### Этап 2 — чтобы хоть что-то работало

**Цель**  
Чтобы можно было запустить брокер и отправить/получить хотя бы пару сообщений.

**Что реально должно заработать**
- можно создать топик или очередь
- паблишер кидает сообщение 
- один или несколько подписчиков получают это сообщение
- сообщения хранятся в памяти 
- базовый stomp 
- клиентская библиотека умеет подключаться и отправлять 1 сообщение + получить 1 сообщение

**Результат**  
Есть прототип

---

### Этап 3 — прикручиваем базу + какая никакая отказоустойчивость

**Цель**  
Перестать терять сообщения при перезапуске и начать приближаться к реальному продукту.

**Что добавляем**
- все сообщения пишутся в postgres
- после рестарта брокера подписчики могут продолжить читать
- реализована логика ACK 
- сообщения не пропадают, даже если клиент отвалился
- хотя бы простая обработка redelivery 
- клиентская либа уже умеет reconnect и не умирает при обрыве связи

**Результат**  
Брокер уже выглядит нормально

---

### Этап 4 — добавляем функций

**Цель**  
Добавить желательного функционала

**Что делаем**
- приоритеты сообщений
- ttl 
- dead letter queue
- prometheus метрики 
- rest api для просмотра состояния 
- простой веб-интерфейс 
- три тестовых микросервиса с логами
- хорошие тесты

**Результат**  
готовый продукт

