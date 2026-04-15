# свой брокер сообщений

простой брокер сообщений с нуля, чтобы три тестовых сервиса могли общаться асинхронно:  
один публикует, другие получают

### фукнции
- pub/sub 
- point-to-point очереди
- сообщения сохраняются в postgres → не теряются после рестарта
- at-least-once доставка
- нормальная python клиентская библиотека 
- stomp over websocket 
- prometheus метрики
- приоритеты + ttl + dead letter queue

### Как запустить 
```bash
docker compose up --build


uvicorn app.main:app --reload

irm -Method POST http://127.0.0.1:8000/subscriptions -ContentType "application/json" -Body '{"destination":"/topic/news.sport"}'

irm -Method POST http://127.0.0.1:8000/topics/news.sport/publish -ContentType "application/json" -Body '{"body":"goal","headers":{"x-id":"1"}}'


python -m pytest -q