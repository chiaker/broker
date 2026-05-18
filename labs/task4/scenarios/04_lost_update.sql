-- =====================================================================
-- АНОМАЛИЯ 4: LOST UPDATE
-- Две транзакции читают одно и то же значение, считают новое и пишут
-- обратно. Та, что закоммитила позже, "перетирает" чужие изменения.
-- =====================================================================
-- Начальное состояние: accounts(id=1).balance = 1000

-- ===== СЕССИЯ A (T1, начисляет +100) =====
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;
SELECT balance FROM accounts WHERE id = 1;    -- 1000

-- ===== СЕССИЯ B (T2, списывает -50) =====
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;
SELECT balance FROM accounts WHERE id = 1;    -- 1000

-- ===== СЕССИЯ A =====
UPDATE accounts SET balance = 1100 WHERE id = 1;   -- 1000 + 100
COMMIT;

-- ===== СЕССИЯ B =====
UPDATE accounts SET balance = 950 WHERE id = 1;    -- 1000 - 50
COMMIT;

-- Итог:
SELECT balance FROM accounts WHERE id = 1;    -- 950 (+100 от T1 ПОТЕРЯН)

-- =====================================================================
-- КАК ИЗБЕЖАТЬ:
--   1) Атомарный UPDATE без read-modify-write:
--        UPDATE accounts SET balance = balance + 100 WHERE id = 1;
--      Самое простое и быстрое — пишем дельту, а не значение.
--   2) Пессимистичная блокировка на чтении:
--        SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;
--      Тогда вторая транзакция ждёт первую.
--   3) Optimistic locking через version/updated_at:
--        UPDATE ... WHERE id = 1 AND version = :old_version;
--      Проверять affected rows, при 0 — повторять.
-- =====================================================================
