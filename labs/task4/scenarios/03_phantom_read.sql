-- =====================================================================
-- АНОМАЛИЯ 3: PHANTOM READ
-- T1 дважды считает строки в диапазоне; между запросами T2 вставляет
-- новую строку, попадающую в этот диапазон. Результат: COUNT меняется.
-- Воспроизводится на READ COMMITTED.
-- =====================================================================
-- Начальное состояние: products в категории 'book' = 5 строк

-- ===== СЕССИЯ A (T1) =====
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;
SELECT COUNT(*) FROM products WHERE category = 'book';   -- 5

-- ===== СЕССИЯ B (T2) =====
START TRANSACTION;
INSERT INTO products (name, price, category)
VALUES ('Phantom Book', 100, 'book');
COMMIT;

-- ===== СЕССИЯ A =====
SELECT COUNT(*) FROM products WHERE category = 'book';   -- 6 (PHANTOM!)
COMMIT;

-- =====================================================================
-- КАК ИЗБЕЖАТЬ:
--   1) Поднять T1 до SERIALIZABLE — повторный COUNT даст 5.
--   2) В REPEATABLE READ обычные snapshot-чтения тоже спасают (MVCC),
--      но locking-чтения (SELECT ... FOR SHARE / FOR UPDATE) в InnoDB
--      берут next-key locks, что блокирует INSERT-фантомы в диапазоне.
-- =====================================================================
