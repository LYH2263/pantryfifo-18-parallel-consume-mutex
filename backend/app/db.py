import os, sqlite3
from pathlib import Path

def db_path() -> Path:
    d = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
    d.mkdir(parents=True, exist_ok=True)
    return d / "pantryfifo.db"

def connect():
    # isolation_level=None: 关闭隐式事务，事务一律由业务代码显式
    # BEGIN IMMEDIATE / COMMIT / ROLLBACK，保证"读快照→扣减"在同一临界区。
    # timeout=10 即 busy_timeout：并发写时后到者等锁而不是立刻 SQLITE_BUSY。
    c = sqlite3.connect(db_path(), timeout=10, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")  # 读写不互斥，层页/余量读取不被扣减阻塞
    c.execute("PRAGMA foreign_keys=ON")
    return c
