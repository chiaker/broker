import os
import time
import threading
from contextlib import contextmanager

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


CACHE_MODE = os.getenv("CACHE_MODE", "lazy")
assert CACHE_MODE in ("lazy", "write_through", "write_back"), CACHE_MODE

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "shop")
DB_USER = os.getenv("DB_USER", "shop_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "shop_password")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))
WB_FLUSH_INTERVAL = float(os.getenv("WB_FLUSH_INTERVAL", "2.0"))
WB_FLUSH_BATCH = int(os.getenv("WB_FLUSH_BATCH", "100"))


db_pool: ThreadedConnectionPool | None = None
r: redis.Redis | None = None

stats_lock = threading.Lock()
stats = {
    "db_reads": 0,
    "db_writes": 0,
    "cache_hits": 0,
    "cache_misses": 0,
    "wb_flush_batches": 0,
}


def bump(key: str, n: int = 1) -> None:
    with stats_lock:
        stats[key] += n


@contextmanager
def db_conn():
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        db_pool.putconn(conn)


def wait_for_db():
    for _ in range(60):
        try:
            c = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                                 user=DB_USER, password=DB_PASSWORD)
            c.close()
            return
        except psycopg2.OperationalError:
            time.sleep(1)
    raise RuntimeError("DB not available")


def key_for(item_id: int) -> str:
    return f"item:{item_id}"


def db_read(item_id: int) -> str | None:
    bump("db_reads")
    with db_conn() as c, c.cursor() as cur:
        cur.execute("select value from items where id = %s", (item_id,))
        row = cur.fetchone()
        return row[0] if row else None


def db_write(item_id: int, value: str) -> None:
    bump("db_writes")
    with db_conn() as c:
        with c.cursor() as cur:
            cur.execute(
                "insert into items (id, value, updated_at) values (%s, %s, now()) "
                "on conflict (id) do update set value = excluded.value, updated_at = now()",
                (item_id, value),
            )
        c.commit()


def db_write_many(pairs: list[tuple[int, str]]) -> None:
    if not pairs:
        return
    bump("db_writes", len(pairs))
    with db_conn() as c:
        with c.cursor() as cur:
            cur.executemany(
                "insert into items (id, value, updated_at) values (%s, %s, now()) "
                "on conflict (id) do update set value = excluded.value, updated_at = now()",
                pairs,
            )
        c.commit()


def read_via_cache(item_id: int) -> str:
    k = key_for(item_id)
    cached = r.get(k)
    if cached is not None:
        bump("cache_hits")
        return cached
    bump("cache_misses")
    value = db_read(item_id)
    if value is None:
        raise HTTPException(status_code=404, detail="not found")
    r.set(k, value, ex=CACHE_TTL)
    return value


def write_lazy(item_id: int, value: str) -> None:
    db_write(item_id, value)
    r.delete(key_for(item_id))


def write_through(item_id: int, value: str) -> None:
    db_write(item_id, value)
    r.set(key_for(item_id), value, ex=CACHE_TTL)


PENDING_SET = "wb:pending"


def write_back(item_id: int, value: str) -> None:
    pipe = r.pipeline()
    pipe.set(key_for(item_id), value, ex=CACHE_TTL)
    pipe.sadd(PENDING_SET, item_id)
    pipe.execute()


def flush_write_back(limit: int | None = None) -> int:
    ids = r.spop(PENDING_SET, limit or WB_FLUSH_BATCH)
    if not ids:
        return 0
    if isinstance(ids, (str, bytes)):
        ids = [ids]
    pairs = []
    for raw in ids:
        item_id = int(raw)
        val = r.get(key_for(item_id))
        if val is not None:
            pairs.append((item_id, val))
    if pairs:
        db_write_many(pairs)
        bump("wb_flush_batches")
    return len(pairs)


wb_thread_stop = threading.Event()


def wb_background():
    while not wb_thread_stop.is_set():
        try:
            size = r.scard(PENDING_SET)
            if size >= WB_FLUSH_BATCH:
                flush_write_back(WB_FLUSH_BATCH)
            else:
                wb_thread_stop.wait(WB_FLUSH_INTERVAL)
                flush_write_back(WB_FLUSH_BATCH)
        except Exception as e:
            print(f"[wb] error: {e}", flush=True)
            wb_thread_stop.wait(1.0)


app = FastAPI()


class WriteBody(BaseModel):
    value: str


@app.on_event("startup")
def startup():
    global db_pool, r
    wait_for_db()
    db_pool = ThreadedConnectionPool(
        minconn=2, maxconn=20,
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                    socket_timeout=2, socket_connect_timeout=2,
                    max_connections=50)
    for _ in range(30):
        try:
            r.ping()
            break
        except redis.ConnectionError:
            time.sleep(1)
    if CACHE_MODE == "write_back":
        t = threading.Thread(target=wb_background, daemon=True)
        t.start()
    print(f"[startup] mode={CACHE_MODE}", flush=True)


@app.on_event("shutdown")
def shutdown():
    wb_thread_stop.set()
    if CACHE_MODE == "write_back":
        try:
            while flush_write_back(1000) > 0:
                pass
        except Exception:
            pass


@app.get("/health")
def health():
    return {"ok": True, "mode": CACHE_MODE}


@app.get("/items/{item_id}")
def get_item(item_id: int):
    value = read_via_cache(item_id)
    return {"id": item_id, "value": value}


@app.put("/items/{item_id}")
def put_item(item_id: int, body: WriteBody):
    if CACHE_MODE == "lazy":
        write_lazy(item_id, body.value)
    elif CACHE_MODE == "write_through":
        write_through(item_id, body.value)
    else:
        write_back(item_id, body.value)
    return {"id": item_id, "value": body.value, "mode": CACHE_MODE}


@app.get("/stats")
def get_stats():
    with stats_lock:
        s = dict(stats)
    hits = s["cache_hits"]
    misses = s["cache_misses"]
    total = hits + misses
    s["hit_rate"] = round(hits / total, 4) if total else 0.0
    s["write_back_queue_size"] = r.scard(PENDING_SET) if r is not None else 0
    s["mode"] = CACHE_MODE
    return s


@app.post("/stats/reset")
def reset_stats():
    with stats_lock:
        for k in stats:
            stats[k] = 0
    return {"ok": True}


@app.post("/admin/flush")
def admin_flush():
    total = 0
    while True:
        n = flush_write_back(1000)
        total += n
        if n == 0:
            break
    return {"flushed": total, "queue_size": r.scard(PENDING_SET)}
