import json
import sqlite3
from datetime import date, datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from app import seed
from app.db import connect, write_tx
from app.engines.fefo import consume_fefo, expire_lots
from app.testing_gate import gate

app = FastAPI(title="Pantryfifo", version="0.2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
gate.register_routes(app)

@app.on_event("startup")
def _startup(): seed.init_db()

@app.get("/api/health")
def health(): return {"ok": True, "project": "pantryfifo"}

def _on_shelf_lots(c, item_id: int) -> list[dict]:
    return [dict(r) for r in c.execute(
        "SELECT * FROM lots WHERE item_id=? AND status='on_shelf' AND qty_remain>0",
        (item_id,))]

def _layer_remaining(c) -> dict:
    """Whole-shelf remaining qty grouped by layer — the 'world' left behind."""
    rows = c.execute(
        """SELECT items.layer AS layer, SUM(lots.qty_remain) AS rem
             FROM lots JOIN items ON items.id=lots.item_id
            WHERE lots.status='on_shelf' AND lots.qty_remain>0 GROUP BY items.layer""")
    return {r["layer"]: round(float(r["rem"]), 3) for r in rows}

def _world(c, item_id: int, observed: list[dict], after: dict[int, float]) -> dict:
    total = round(sum(after.values()), 3)
    return {
        "item_id": item_id,
        "observed_lots": observed,
        "lot_remain_after": {str(k): round(v, 3) for k, v in after.items()},
        "item_remain_after": total,
        "layer_remain_after": _layer_remaining(c),
    }

@app.get("/api/items")
def items():
    c = connect(); rows = [dict(r) for r in c.execute("SELECT * FROM items")]; c.close(); return rows

@app.get("/api/fridge")
def fridge(layer: str | None = None):
    c = connect()
    q = """SELECT lots.*, items.name, items.layer, items.unit FROM lots
           JOIN items ON items.id=lots.item_id WHERE lots.status='on_shelf'"""
    args = []
    if layer:
        q += " AND items.layer=?"; args.append(layer)
    rows = [dict(r) for r in c.execute(q, args)]; c.close(); return rows

@app.get("/api/alerts")
def alerts():
    c = connect()
    warn = int(c.execute("SELECT value FROM settings WHERE key='warn_days'").fetchone()["value"])
    today = date.today().isoformat()
    rows = [dict(r) for r in c.execute(
        """SELECT lots.*, items.name, items.layer FROM lots JOIN items ON items.id=lots.item_id
           WHERE status='on_shelf' AND qty_remain>0 AND expiry IS NOT NULL""")]
    c.close()
    out = []
    for r in rows:
        if r["expiry"] <= today:
            r["level"] = "expired"
            out.append(r)
        else:
            # simple day diff via fromisoformat
            delta = (date.fromisoformat(r["expiry"]) - date.today()).days
            if delta <= warn:
                r["level"] = "soon"; r["days_left"] = delta; out.append(r)
    return out

class LotIn(BaseModel):
    item_id: int
    qty: float
    expiry: str
    gate_tag: str | None = None

@app.post("/api/lots")
def inbound(body: LotIn):
    # The writer lock makes inbound the canonical "third road": a consume that
    # waits behind it plans FEFO against the shelf containing this new lot.
    gate.wait("inbound", body.gate_tag)
    with write_tx() as c:
        item = c.execute("SELECT id FROM items WHERE id=?", (body.item_id,)).fetchone()
        if not item:
            raise HTTPException(404, "item")
        cur = c.execute(
            "INSERT INTO lots(item_id,qty_in,qty_remain,expiry,status,data_quality) VALUES (?,?,?,?,?,?)",
            (body.item_id, body.qty, body.qty, body.expiry, "on_shelf", "clean"))
        lid = cur.lastrowid
    return {"id": lid}

class ConsumeIn(BaseModel):
    item_id: int
    qty: float
    note: str = ""
    token: str | None = None
    gate_tag: str | None = None

@app.post("/api/consume")
def consume(body: ConsumeIn):
    gate.wait("consume", body.gate_tag)
    try:
        with write_tx() as c:
            # Idempotent confirm: a retried double-click of the same gesture replays
            # the committed result instead of deducting twice.
            if body.token:
                seen = c.execute(
                    "SELECT result_json FROM consume_tokens WHERE token=?", (body.token,)).fetchone()
                if seen:
                    stored = json.loads(seen["result_json"])
                    stored["replayed"] = True
                    return stored

            # Fresh read INSIDE the writer lock. Any earlier consume / inbound /
            # sweep has already committed, so this is the single world we plan on.
            lots = _on_shelf_lots(c, body.item_id)
            observed = [{"lot_id": l["id"], "expiry": l.get("expiry"),
                         "qty_remain": float(l["qty_remain"])} for l in lots]
            result = consume_fefo(lots, body.qty)
            result["observed_lots"] = observed

            if not result["ok"] and result["reason"] == "qty_non_positive":
                raise HTTPException(400, result["reason"])

            if not result["ok"]:
                # Plan short against THIS world: apply nothing, tx rolls back.
                # World snapshot lets the caller verify lots/layer pages match the
                # single surviving outcome instead of some half-applied state.
                after = {l["id"]: float(l["qty_remain"]) for l in lots}
                result["world"] = _world(c, body.item_id, observed, after)
                raise HTTPException(409, result)

            after: dict[int, float] = {l["id"]: float(l["qty_remain"]) for l in lots}
            for d in result["deductions"]:
                # Guarded update: never drive qty_remain negative even if the read
                # and plan somehow disagree. The writer lock already prevents it.
                cur = c.execute(
                    "UPDATE lots SET qty_remain = qty_remain - ? WHERE id=? AND qty_remain >= ?",
                    (d["take"], d["lot_id"], d["take"]))
                if cur.rowcount != 1:
                    raise HTTPException(409, {"ok": False, "reason": "concurrent_change",
                                              "deductions": [], "short": float(body.qty)})
                rem = c.execute("SELECT qty_remain FROM lots WHERE id=?",
                                (d["lot_id"],)).fetchone()["qty_remain"]
                if rem <= 1e-9:
                    c.execute("UPDATE lots SET status='consumed', qty_remain=0 WHERE id=?",
                              (d["lot_id"],))
                    rem = 0.0
                after[d["lot_id"]] = float(rem)

            result["world"] = _world(c, body.item_id, observed, after)
            c.execute("INSERT INTO consumptions(note,result_json,created_at) VALUES (?,?,?)",
                      (body.note, json.dumps(result), datetime.now(timezone.utc).isoformat()))
            if body.token:
                c.execute("INSERT INTO consume_tokens(token,result_json,created_at) VALUES (?,?,?)",
                          (body.token, json.dumps(result), datetime.now(timezone.utc).isoformat()))
            return result
    except sqlite3.IntegrityError:
        # Two identical tokens raced past the SELECT together; PRIMARY KEY makes
        # the loser roll back whole. Replay the winner's committed world.
        if not body.token:
            raise
        c = connect()
        row = c.execute("SELECT result_json FROM consume_tokens WHERE token=?",
                        (body.token,)).fetchone()
        c.close()
        if not row:
            raise
        stored = json.loads(row["result_json"])
        stored["replayed"] = True
        return stored

@app.post("/api/expire-sweep")
def expire_sweep(gate_tag: str | None = None):
    # Other "third road": taking the earliest lot off the shelf commits first,
    # so a waiting consume plans FEFO without it.
    gate.wait("sweep", gate_tag)
    with write_tx() as c:
        lots = [dict(r) for r in c.execute("SELECT * FROM lots WHERE status='on_shelf'")]
        ids = expire_lots(lots, date.today().isoformat())
        for i in ids:
            c.execute("UPDATE lots SET status='expired' WHERE id=?", (i,))
    return {"expired_ids": ids}

@app.get("/api/settings")
def settings():
    c = connect(); rows = {r["key"]: r["value"] for r in c.execute("SELECT * FROM settings")}; c.close(); return rows
