import io
import os
import sys
from pathlib import Path

import pymysql
from tabulate import tabulate

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3307"))
DB_USER = os.getenv("DB_USER", "iso")
DB_PASSWORD = os.getenv("DB_PASSWORD", "iso_pwd")
DB_NAME = os.getenv("DB_NAME", "isolab")

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)


def connect(label: str):
    c = pymysql.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
        autocommit=False, charset="utf8mb4",
    )
    c._label = label
    return c


class Log:
    def __init__(self, path: Path):
        self.fp = open(path, "w", encoding="utf-8")

    def write(self, msg: str = ""):
        print(msg)
        self.fp.write(msg + "\n")

    def close(self):
        self.fp.close()


def step(log: Log, conn, sql: str, note: str = ""):
    label = conn._label
    head = f"[{label}] {sql.strip()}"
    if note:
        head += f"   -- {note}"
    log.write(head)
    with conn.cursor() as cur:
        cur.execute(sql)
        if cur.description:
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            log.write(tabulate(rows, headers=cols, tablefmt="github"))
        else:
            log.write(f"   -> affected rows: {cur.rowcount}")
    log.write("")


def commit(log: Log, conn):
    log.write(f"[{conn._label}] COMMIT")
    conn.commit()
    log.write("")


def rollback(log: Log, conn):
    log.write(f"[{conn._label}] ROLLBACK")
    conn.rollback()
    log.write("")


def reset_db():
    c = pymysql.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
        autocommit=True, charset="utf8mb4",
    )
    with c.cursor() as cur:
        cur.execute("DELETE FROM accounts")
        cur.execute("DELETE FROM products")
        cur.execute("ALTER TABLE products AUTO_INCREMENT = 1")
        cur.execute("INSERT INTO accounts (id, owner, balance) VALUES "
                    "(1,'Alice',1000),(2,'Bob',500)")
        cur.execute("INSERT INTO products (name, price, category) VALUES "
                    "('SQL Antipatterns',500,'book'),"
                    "('Designing Data Intensive',700,'book'),"
                    "('PostgreSQL Up and Running',600,'book'),"
                    "('MySQL Cookbook',550,'book'),"
                    "('Database Internals',650,'book')")
    c.close()


def banner(log: Log, title: str):
    log.write("=" * 70)
    log.write(title)
    log.write("=" * 70)


# ---------------------------------------------------------------------------

def demo_dirty_read():
    log = Log(RESULTS_DIR / "01_dirty_read.txt")
    banner(log, "АНОМАЛИЯ: DIRTY READ  (MySQL READ UNCOMMITTED)")
    reset_db()
    t1 = connect("T1"); t2 = connect("T2")
    try:
        step(log, t1, "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
             note="T1 будет видеть незакоммиченные данные")
        step(log, t1, "START TRANSACTION")
        step(log, t1, "SELECT balance FROM accounts WHERE id=1",
             note="до правок T2")

        step(log, t2, "START TRANSACTION")
        step(log, t2, "UPDATE accounts SET balance = balance - 200 WHERE id=1",
             note="НЕ коммитим")

        step(log, t1, "SELECT balance FROM accounts WHERE id=1",
             note="DIRTY READ: видим незакоммиченные 800")

        rollback(log, t2)

        step(log, t1, "SELECT balance FROM accounts WHERE id=1",
             note="после ROLLBACK T2 значение снова 1000")
        commit(log, t1)
    finally:
        t1.close(); t2.close()

    # FIX
    banner(log, "FIX: тот же сценарий, но T1 на READ COMMITTED")
    reset_db()
    t1 = connect("T1"); t2 = connect("T2")
    try:
        step(log, t1, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        step(log, t1, "START TRANSACTION")
        step(log, t2, "START TRANSACTION")
        step(log, t2, "UPDATE accounts SET balance = balance - 200 WHERE id=1")
        step(log, t1, "SELECT balance FROM accounts WHERE id=1",
             note="видим 1000 — грязное чтение исключено")
        rollback(log, t2)
        commit(log, t1)
    finally:
        t1.close(); t2.close()
    log.close()


def demo_non_repeatable_read():
    log = Log(RESULTS_DIR / "02_non_repeatable_read.txt")
    banner(log, "АНОМАЛИЯ: NON-REPEATABLE READ  (READ COMMITTED)")
    reset_db()
    t1 = connect("T1"); t2 = connect("T2")
    try:
        step(log, t1, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        step(log, t2, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        step(log, t1, "START TRANSACTION")
        step(log, t1, "SELECT balance FROM accounts WHERE id=1",
             note="первое чтение")
        step(log, t2, "START TRANSACTION")
        step(log, t2, "UPDATE accounts SET balance = 1500 WHERE id=1")
        commit(log, t2)
        step(log, t1, "SELECT balance FROM accounts WHERE id=1",
             note="NON-REPEATABLE: значение в той же T1 уже другое")
        commit(log, t1)
    finally:
        t1.close(); t2.close()

    banner(log, "FIX: T1 на REPEATABLE READ — snapshot фиксируется на старте")
    reset_db()
    t1 = connect("T1"); t2 = connect("T2")
    try:
        step(log, t1, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        step(log, t1, "START TRANSACTION")
        step(log, t1, "SELECT balance FROM accounts WHERE id=1",
             note="закрепляем snapshot")
        step(log, t2, "START TRANSACTION")
        step(log, t2, "UPDATE accounts SET balance = 1500 WHERE id=1")
        commit(log, t2)
        step(log, t1, "SELECT balance FROM accounts WHERE id=1",
             note="всё ещё 1000 — повторное чтение стабильно")
        commit(log, t1)
    finally:
        t1.close(); t2.close()
    log.close()


def demo_phantom_read():
    log = Log(RESULTS_DIR / "03_phantom_read.txt")
    banner(log, "АНОМАЛИЯ: PHANTOM READ  (READ COMMITTED)")
    reset_db()
    t1 = connect("T1"); t2 = connect("T2")
    try:
        step(log, t1, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        step(log, t1, "START TRANSACTION")
        step(log, t1, "SELECT COUNT(*) AS cnt FROM products WHERE category='book'",
             note="ожидаем 5")
        step(log, t2, "START TRANSACTION")
        step(log, t2, "INSERT INTO products (name, price, category) "
                      "VALUES ('Phantom Book', 100, 'book')")
        commit(log, t2)
        step(log, t1, "SELECT COUNT(*) AS cnt FROM products WHERE category='book'",
             note="PHANTOM: появилась новая строка в диапазоне")
        commit(log, t1)
    finally:
        t1.close(); t2.close()

    banner(log, "FIX: T1 на SERIALIZABLE — COUNT стабильный")
    reset_db()
    t1 = connect("T1"); t2 = connect("T2")
    try:
        step(log, t1, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        step(log, t1, "START TRANSACTION")
        step(log, t1, "SELECT COUNT(*) AS cnt FROM products WHERE category='book'")
        log.write("[T2] попытка INSERT в этот же диапазон будет ждать "
                  "коммита T1 (демонстрируем словесно, чтобы не зависнуть).")
        log.write("")
        step(log, t1, "SELECT COUNT(*) AS cnt FROM products WHERE category='book'",
             note="всё ещё 5 — фантомов нет")
        commit(log, t1)
    finally:
        t1.close(); t2.close()
    log.close()


def demo_lost_update():
    log = Log(RESULTS_DIR / "04_lost_update.txt")
    banner(log, "АНОМАЛИЯ: LOST UPDATE  (read-modify-write race)")
    reset_db()
    t1 = connect("T1"); t2 = connect("T2")
    try:
        step(log, t1, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        step(log, t2, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        step(log, t1, "START TRANSACTION")
        step(log, t2, "START TRANSACTION")
        step(log, t1, "SELECT balance FROM accounts WHERE id=1",
             note="T1 прочитал 1000, хочет +100")
        step(log, t2, "SELECT balance FROM accounts WHERE id=1",
             note="T2 прочитал 1000, хочет -50")
        step(log, t1, "UPDATE accounts SET balance = 1100 WHERE id=1")
        commit(log, t1)
        step(log, t2, "UPDATE accounts SET balance = 950 WHERE id=1",
             note="перетёр +100 от T1")
        commit(log, t2)
        c = connect("CHECK")
        step(log, c, "SELECT balance FROM accounts WHERE id=1",
             note="итог = 950, изменение T1 ПОТЕРЯНО (ожидалось 1050)")
        c.close()
    finally:
        t1.close(); t2.close()

    banner(log, "FIX 1: атомарный UPDATE без read-modify-write")
    reset_db()
    t1 = connect("T1"); t2 = connect("T2")
    try:
        step(log, t1, "START TRANSACTION")
        step(log, t2, "START TRANSACTION")
        step(log, t1, "UPDATE accounts SET balance = balance + 100 WHERE id=1")
        commit(log, t1)
        step(log, t2, "UPDATE accounts SET balance = balance - 50 WHERE id=1")
        commit(log, t2)
        c = connect("CHECK")
        step(log, c, "SELECT balance FROM accounts WHERE id=1",
             note="итог = 1050 — обе операции применились")
        c.close()
    finally:
        t1.close(); t2.close()

    banner(log, "FIX 2: пессимистичная блокировка — SELECT ... FOR UPDATE")
    reset_db()
    t1 = connect("T1"); t2 = connect("T2")
    try:
        step(log, t1, "START TRANSACTION")
        step(log, t1, "SELECT balance FROM accounts WHERE id=1 FOR UPDATE",
             note="T1 захватил X-lock на строку")
        log.write("[T2] SELECT ... FOR UPDATE будет ждать освобождения "
                  "локa T1 (демонстрируем словесно, чтобы не зависнуть).")
        log.write("")
        step(log, t1, "UPDATE accounts SET balance = balance + 100 WHERE id=1")
        commit(log, t1)
        step(log, t2, "START TRANSACTION")
        step(log, t2, "SELECT balance FROM accounts WHERE id=1 FOR UPDATE",
             note="теперь T2 видит уже 1100, считает от него")
        step(log, t2, "UPDATE accounts SET balance = balance - 50 WHERE id=1")
        commit(log, t2)
        c = connect("CHECK")
        step(log, c, "SELECT balance FROM accounts WHERE id=1",
             note="итог = 1050 — потери нет")
        c.close()
    finally:
        t1.close(); t2.close()
    log.close()


# ---------------------------------------------------------------------------

def main():
    print(f"connecting to {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    demos = [demo_dirty_read, demo_non_repeatable_read,
             demo_phantom_read, demo_lost_update]
    for d in demos:
        print()
        d()
    print(f"\nDone. Logs in: {RESULTS_DIR}")


if __name__ == "__main__":
    sys.exit(main())
