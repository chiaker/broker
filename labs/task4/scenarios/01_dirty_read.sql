-- =====================================================================
-- АНОМАЛИЯ 1: DIRTY READ
-- Транзакция T1 читает данные, которые T2 ещё не закоммитила.
-- Воспроизводится в MySQL InnoDB на уровне READ UNCOMMITTED.
-- =====================================================================
-- Начальное состояние: accounts(id=1).balance = 1000

-- ===== СЕССИЯ A (T1, читатель) =====
SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
START TRANSACTION;
-- (1) до правок T2:
SELECT balance FROM accounts WHERE id = 1;    -- 1000

-- ===== СЕССИЯ B (T2, писатель) =====
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION;
UPDATE accounts SET balance = balance - 200 WHERE id = 1;   -- НЕ коммитим

-- ===== СЕССИЯ A =====
-- (2) видим грязные данные T2:
SELECT balance FROM accounts WHERE id = 1;    -- 800 (DIRTY!)

-- ===== СЕССИЯ B =====
ROLLBACK;

-- ===== СЕССИЯ A =====
-- (3) после rollback значение опять "настоящее":
SELECT balance FROM accounts WHERE id = 1;    -- 1000
COMMIT;

-- =====================================================================
-- КАК ИЗБЕЖАТЬ: использовать READ COMMITTED или выше на читателе.
-- На READ COMMITTED шаг (2) вернёт 1000 — грязное чтение исключено.
-- =====================================================================
