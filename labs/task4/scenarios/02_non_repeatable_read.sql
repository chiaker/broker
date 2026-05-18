-- =====================================================================
-- АНОМАЛИЯ 2: NON-REPEATABLE READ
-- T1 дважды читает одну и ту же строку, а между чтениями T2 успевает
-- закоммитить UPDATE. Результат: разные значения в одной транзакции.
-- Воспроизводится на READ COMMITTED.
-- =====================================================================
-- Начальное состояние: accounts(id=1).balance = 1000

-- ===== СЕССИЯ A (T1) =====
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;
SELECT balance FROM accounts WHERE id = 1;    -- 1000

-- ===== СЕССИЯ B (T2) =====
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;
UPDATE accounts SET balance = 1500 WHERE id = 1;
COMMIT;

-- ===== СЕССИЯ A =====
SELECT balance FROM accounts WHERE id = 1;    -- 1500 (изменилось!)
COMMIT;

-- =====================================================================
-- КАК ИЗБЕЖАТЬ: поднять уровень изоляции T1 до REPEATABLE READ.
-- В MySQL InnoDB RR создаёт consistent snapshot на старте транзакции,
-- и оба SELECT в T1 вернут одно и то же значение (1000).
-- =====================================================================
