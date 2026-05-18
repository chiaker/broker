# Отчёт: аномалии изоляции в SQL

## Что показываем

4 аномалии параллельных транзакций на **MySQL 8 / InnoDB**: dirty read, non-repeatable read, phantom read, lost update. Оркестратор — Python с двумя `pymysql`-коннектами (`autocommit=False`), пошагово гоняет T1 и T2 и пишет логи в [results/](results/). Для каждой аномалии тут же показан фикс — повторяем те же шаги, но с более строгим уровнем изоляции или с правильной блокировкой, и убеждаемся, что аномалия не воспроизводится.

## Сетап

```sql
-- init.sql
accounts (id, owner, balance)
  (1, 'Alice', 1000),
  (2, 'Bob',    500);

products (id, name, price, category)
  5 строк в категории 'book'
```

## 1. Dirty Read

T1 на `READ UNCOMMITTED` читает то, что T2 ещё не закоммитила. После `ROLLBACK` у T2 видение T1 становилось «псевдо-правдой» — это и есть грязное чтение.

| шаг | T1 (READ UNCOMMITTED) | T2 (REPEATABLE READ) |
| :-- | :-- | :-- |
| 1 | `BEGIN; SELECT balance` → **1000** | |
| 2 | | `BEGIN; UPDATE balance-=200` (не commit) |
| 3 | `SELECT balance` → **800** (грязное) | |
| 4 | | `ROLLBACK` |
| 5 | `SELECT balance` → **1000** | |

Лог: [results/01_dirty_read.txt](results/01_dirty_read.txt).

```
[T2] UPDATE accounts SET balance = balance - 200 WHERE id=1   -- НЕ коммитим
[T1] SELECT balance FROM accounts WHERE id=1   -- DIRTY READ: видим незакоммиченные 800
|   balance |
|-----------|
|       800 |
[T2] ROLLBACK
[T1] SELECT balance FROM accounts WHERE id=1   -- после ROLLBACK значение снова 1000
|   balance |
|-----------|
|      1000 |
```

**Как избежать:** не использовать `READ UNCOMMITTED`. Уже на `READ COMMITTED` повторный шаг 3 возвращает 1000 — в логе видно, фикс отработал.

## 2. Non-Repeatable Read

T1 на `READ COMMITTED` дважды читает одну строку, между чтениями T2 успевает закоммитить `UPDATE`. Результат — два разных значения в одной транзакции.

| шаг | T1 (READ COMMITTED) | T2 (READ COMMITTED) |
| :-- | :-- | :-- |
| 1 | `BEGIN; SELECT balance` → **1000** | |
| 2 | | `BEGIN; UPDATE balance=1500; COMMIT` |
| 3 | `SELECT balance` → **1500** | |

Лог: [results/02_non_repeatable_read.txt](results/02_non_repeatable_read.txt).

```
[T1] SELECT balance FROM accounts WHERE id=1   -- первое чтение
|   balance |
|-----------|
|      1000 |
[T2] UPDATE accounts SET balance = 1500 WHERE id=1
[T2] COMMIT
[T1] SELECT balance FROM accounts WHERE id=1   -- NON-REPEATABLE: значение в той же T1 уже другое
|   balance |
|-----------|
|      1500 |
```

**Как избежать:** поднять T1 до `REPEATABLE READ`. В MySQL InnoDB на этом уровне создаётся consistent snapshot на старте транзакции, и оба `SELECT` возвращают одно и то же. В фикс-блоке лога повторное чтение даёт 1000.

## 3. Phantom Read

T1 на `READ COMMITTED` дважды считает строки в диапазоне; между запросами T2 коммитит `INSERT`, попадающий в этот диапазон. Появляется новая строка-«фантом».

| шаг | T1 (READ COMMITTED) | T2 |
| :-- | :-- | :-- |
| 1 | `BEGIN; SELECT COUNT(*) WHERE category='book'` → **5** | |
| 2 | | `INSERT 'Phantom Book' INTO products; COMMIT` |
| 3 | `SELECT COUNT(*) WHERE category='book'` → **6** | |

Лог: [results/03_phantom_read.txt](results/03_phantom_read.txt).

```
[T1] SELECT COUNT(*) AS cnt FROM products WHERE category='book'   -- ожидаем 5
|   cnt |
|-------|
|     5 |
[T2] INSERT INTO products (name, price, category) VALUES ('Phantom Book', 100, 'book')
[T2] COMMIT
[T1] SELECT COUNT(*) AS cnt FROM products WHERE category='book'   -- PHANTOM
|   cnt |
|-------|
|     6 |
```

**Как избежать:** `SERIALIZABLE` на T1 — повторный `COUNT(*)` стабилен, а T2 будет ждать коммита T1. На `REPEATABLE READ` обычные snapshot-чтения тоже спасают (MVCC), а блокирующие — за счёт next-key locks InnoDB.

## 4. Lost Update

Классический race: обе транзакции читают `balance=1000`, считают новое значение и пишут обратно. Та, что закоммитила позже, перетирает чужое изменение.

| шаг | T1 (хочет +100) | T2 (хочет -50) |
| :-- | :-- | :-- |
| 1 | `BEGIN; SELECT balance` → 1000 | `BEGIN; SELECT balance` → 1000 |
| 2 | `UPDATE balance=1100; COMMIT` | |
| 3 | | `UPDATE balance=950; COMMIT` |
| 4 | итог: **950** (ожидалось 1050) | |

Лог: [results/04_lost_update.txt](results/04_lost_update.txt).

```
[T1] SELECT balance FROM accounts WHERE id=1   -- T1 прочитал 1000, хочет +100
[T2] SELECT balance FROM accounts WHERE id=1   -- T2 прочитал 1000, хочет -50
[T1] UPDATE accounts SET balance = 1100 WHERE id=1
[T1] COMMIT
[T2] UPDATE accounts SET balance = 950 WHERE id=1   -- перетёр +100 от T1
[T2] COMMIT
[CHECK] SELECT balance FROM accounts WHERE id=1   -- итог = 950, изменение T1 ПОТЕРЯНО
|   balance |
|-----------|
|       950 |
```

**Как избежать (в порядке предпочтения):**

1. **Атомарный `UPDATE` без read-modify-write** — пишем дельту, а не значение:
   ```sql
   UPDATE accounts SET balance = balance + 100 WHERE id = 1;
   ```
   В фикс-блоке итог = **1050** — обе операции применились.
2. **Пессимистичная блокировка** `SELECT ... FOR UPDATE` — вторая транзакция ждёт первую. Тоже **1050** в логе.
3. **Оптимистичная блокировка** через `version` / `updated_at`: `UPDATE ... WHERE id=1 AND version=:old` + проверка `affected rows`.

## Сводно

| Аномалия              | Воспроизводится на          | Чем закрывается                                       |
| :--                   | :--                         | :--                                                   |
| Dirty read            | `READ UNCOMMITTED`          | `READ COMMITTED` и выше                               |
| Non-repeatable read   | `READ COMMITTED`            | `REPEATABLE READ` (или `SELECT ... FOR SHARE`)        |
| Phantom read          | `READ COMMITTED` (snapshot) | `SERIALIZABLE` / `REPEATABLE READ` + locking-чтение   |
| Lost update           | любой `READ*` без локов     | атомарный `UPDATE`, `FOR UPDATE`, optimistic lock     |

## Как запустить

```powershell
cd labs\task4
docker compose up -d
python -m pip install -r requirements.txt
$env:PYTHONIOENCODING = "utf-8"
python demo.py
```

Логи каждого сценария лягут в [results/](results/). Также можно вручную пройти SQL в двух сессиях `mysql` CLI, сценарии расписаны в [scenarios/](scenarios/).
