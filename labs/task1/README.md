# Решение по транзакциям интернет-магазина

Проект реализует 3 транзакционных сценария для схемы:

- `Customers (CustomerID, FirstName, LastName, Email)`
- `Products (ProductID, ProductName, Price)`
- `Orders (OrderID, CustomerID, OrderDate, TotalAmount)`
- `OrderItems (OrderItemID, OrderID, ProductID, Quantity, Subtotal)`

## Что реализовано

### Сценарий 1: размещение заказа

В рамках одной транзакции:

1. Вставляется запись в `Orders` с `TotalAmount = 0`.
2. Для каждой позиции:
   - считывается цена из `Products`,
   - вычисляется `Subtotal = Price * Quantity`,
   - вставляется строка в `OrderItems`.
3. В `Orders.TotalAmount` записывается сумма всех `Subtotal` из `OrderItems` по созданному заказу.

Если любой шаг падает, транзакция откатывается целиком.

### Сценарий 2: обновление email клиента

В рамках транзакции выполняется `UPDATE Customers SET Email = ... WHERE CustomerID = ... RETURNING ...`.

Если клиента нет или возникает ошибка ограничений (например, `UNIQUE`), изменение не фиксируется.

### Сценарий 3: добавление продукта

В рамках транзакции выполняется вставка в `Products` и возврат созданной записи.

При ошибке вставка не фиксируется.

## Структура

- `app.py` — скрипт с тремя транзакционными сценариями
- `init.sql` — создание таблиц и тестовые данные
- `Dockerfile` — контейнер для Python-скрипта
- `docker-compose.yml` — запуск PostgreSQL и приложения

## Запуск

```bash
docker compose up --build
```

После старта:

- поднимется PostgreSQL,
- выполнится `init.sql`,
- приложение подключится к БД и последовательно выполнит все 3 сценария,
- в логах выведутся результаты и содержимое таблиц.
