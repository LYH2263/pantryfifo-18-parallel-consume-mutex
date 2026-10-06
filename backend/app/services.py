"""写路径事务层：所有"读后写"都在 BEGIN IMMEDIATE 临界区内完成。

互斥拍板：first-wins。先拿锁者基于当前世界扣减并提交；后到者在锁内
重新读到的已是新世界，余量不足即 ConflictError，整笔回滚——
不会留下半截 qty_remain。第三路（并发入库更早批 / 并发下架最早批）
同走这把锁，与扣减互为串行序，不存在杂交世界。
"""
import json
from datetime import date, datetime, timezone

from app.db import connect
from app.engines.fefo import consume_fefo, expire_lots


class ConflictError(Exception):
    """余量不足或批次已变化：整笔未扣，可提示后重试。"""

    def __init__(self, result: dict):
        super().__init__(result.get("reason") or "conflict")
        self.result = result


class BadRequestError(Exception):
    pass


class NotFoundError(Exception):
    pass


def _rollback_quiet(c):
    try:
        c.execute("ROLLBACK")
    except Exception:
        pass  # 事务可能已结束；兜底只为不留半截


def do_consume(item_id: int, qty: float, note: str = "") -> dict:
    """FEFO 扣减。成功返回 result（含 deductions）；冲突抛 ConflictError。"""
    c = connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        lots = [dict(r) for r in c.execute(
            "SELECT * FROM lots WHERE item_id=? AND status='on_shelf' AND qty_remain>0",
            (item_id,))]
        result = consume_fefo(lots, qty)
        if not result["ok"]:
            if result["reason"] == "qty_non_positive":
                raise BadRequestError("qty_non_positive")
            raise ConflictError(result)
        for d in result["deductions"]:
            # CAS 兜底：锁内世界不会变，rowcount 必为 1；若锁策略未来被改，
            # 这里仍能把"快照过期"挡下来并整笔回滚。
            cur = c.execute(
                "UPDATE lots SET qty_remain = qty_remain - ? "
                "WHERE id=? AND status='on_shelf' AND qty_remain >= ?",
                (d["take"], d["lot_id"], d["take"]))
            if cur.rowcount != 1:
                raise ConflictError({"ok": False, "reason": "lot_changed",
                                     "deductions": [], "short": 0.0})
            rem = c.execute("SELECT qty_remain FROM lots WHERE id=?",
                            (d["lot_id"],)).fetchone()["qty_remain"]
            if rem <= 1e-9:
                c.execute("UPDATE lots SET status='consumed', qty_remain=0 WHERE id=?",
                          (d["lot_id"],))
        c.execute("INSERT INTO consumptions(note,result_json,created_at) VALUES (?,?,?)",
                  (note, json.dumps(result), datetime.now(timezone.utc).isoformat()))
        c.execute("COMMIT")
        return result
    except Exception:
        _rollback_quiet(c)
        raise
    finally:
        c.close()


def do_inbound(item_id: int, qty: float, expiry: str) -> dict:
    c = connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        if not c.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone():
            raise NotFoundError("item")
        cur = c.execute(
            "INSERT INTO lots(item_id,qty_in,qty_remain,expiry,status,data_quality) "
            "VALUES (?,?,?,?,?,?)",
            (item_id, qty, qty, expiry, "on_shelf", "clean"))
        lid = cur.lastrowid
        c.execute("COMMIT")
        return {"id": lid}
    except Exception:
        _rollback_quiet(c)
        raise
    finally:
        c.close()


def do_expire_sweep(today: str | None = None) -> dict:
    today = today or date.today().isoformat()
    c = connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        lots = [dict(r) for r in c.execute("SELECT * FROM lots WHERE status='on_shelf'")]
        ids = expire_lots(lots, today)
        for i in ids:
            c.execute("UPDATE lots SET status='expired' WHERE id=? AND status='on_shelf'", (i,))
        c.execute("COMMIT")
        return {"expired_ids": ids}
    except Exception:
        _rollback_quiet(c)
        raise
    finally:
        c.close()
