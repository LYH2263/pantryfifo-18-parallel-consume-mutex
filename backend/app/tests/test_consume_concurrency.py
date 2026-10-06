"""并行扣减互斥测例。

拍板：first-wins —— 重叠的两笔确认里，先拿锁者改 lots 并提交；后到者
在锁内基于新世界重新判定，余量不足即冲突、整笔回滚，不留半截 qty_remain。
第三路（入库刚补更早到期的批 / 下架刚拿走最早那批）与扣减共用同一把
BEGIN IMMEDIATE 锁，互为串行序，只允许出现两种合法世界之一。

每个断言消息都带 [场景名]，失败即打印场景。
"""
import threading

import pytest

from app import seed
from app.db import connect
from app.services import ConflictError, do_consume, do_expire_sweep, do_inbound

TODAY = "2026-10-05"
ROUNDS = 5  # 竞态场景重复轮次：每轮独立数据，提高重叠复现的覆盖


@pytest.fixture()
def fresh_db(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    seed.init_db()
    c = connect()  # 清掉种子批/账，只留空世界，各场景自造数据
    c.execute("DELETE FROM lots")
    c.execute("DELETE FROM consumptions")
    c.close()
    yield


# ---------- helpers ----------

def add_item(name="测试奶", layer="upper", unit="盒"):
    c = connect()
    cur = c.execute("INSERT INTO items(name,layer,unit) VALUES (?,?,?)", (name, layer, unit))
    iid = cur.lastrowid
    c.close()
    return iid


def add_lot(item_id, qty, expiry):
    c = connect()
    cur = c.execute(
        "INSERT INTO lots(item_id,qty_in,qty_remain,expiry,status,data_quality) "
        "VALUES (?,?,?,?,?,?)",
        (item_id, qty, qty, expiry, "on_shelf", "clean"))
    lid = cur.lastrowid
    c.close()
    return lid


def lots_of(item_id):
    c = connect()
    rows = [dict(r) for r in c.execute(
        "SELECT * FROM lots WHERE item_id=? ORDER BY id", (item_id,))]
    c.close()
    return rows


def consumptions_count():
    c = connect()
    n = c.execute("SELECT COUNT(*) c FROM consumptions").fetchone()["c"]
    c.close()
    return n


def run_parallel(*fns):
    """Barrier 同步起跑，让重叠真实发生；返回 [('ok', 值) | ('err', 异常), ...]"""
    barrier = threading.Barrier(len(fns))
    out = [None] * len(fns)

    def _run(i, fn):
        barrier.wait(timeout=10)
        try:
            out[i] = ("ok", fn())
        except Exception as e:
            out[i] = ("err", e)

    ts = [threading.Thread(target=_run, args=(i, f)) for i, f in enumerate(fns)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=30)
    assert all(not t.is_alive() for t in ts), "有线程未结束，疑似死锁"
    return out


def on_shelf_total(lots):
    return sum(l["qty_remain"] for l in lots if l["status"] == "on_shelf")


def assert_deductions_reconcile(scn, result, lots):
    """deductions 必须对得上留下的世界：每批 账面减少 == 该批被扣量。"""
    for l in lots:
        took = sum(d["take"] for d in result["deductions"] if d["lot_id"] == l["id"])
        assert l["qty_in"] - l["qty_remain"] == pytest.approx(took), (
            f"[{scn}] lot{l['id']} 账面减少 {l['qty_in'] - l['qty_remain']} "
            f"与 deductions 扣量 {took} 对不上")


# ---------- 场景 1：两笔重叠，和 > 在架余量 → 只留下一种结果 ----------

def test_overshoot_two_confirms_leave_exactly_one_result(fresh_db):
    SCN = "overshoot_two_confirms"
    for rnd in range(ROUNDS):
        iid = add_item()
        add_lot(iid, 3, "2026-11-01")
        base = consumptions_count()
        out = run_parallel(lambda: do_consume(iid, 2), lambda: do_consume(iid, 2))
        oks = [v for tag, v in out if tag == "ok"]
        errs = [v for tag, v in out if tag == "err"]
        tag = f"[{SCN}#{rnd}]"
        assert len(oks) == 1 and len(errs) == 1, f"{tag} 应恰好一成一败: {out}"
        assert isinstance(errs[0], ConflictError), f"{tag} 败方应为冲突: {errs[0]!r}"
        lots = lots_of(iid)
        assert all(l["qty_remain"] >= 0 for l in lots), f"{tag} 出现负余量: {lots}"
        assert on_shelf_total(lots) == pytest.approx(1), f"{tag} 余量应为 3-2=1: {lots}"
        assert consumptions_count() - base == 1, f"{tag} 只应入账一笔"
        assert_deductions_reconcile(tag, oks[0], lots)


# ---------- 场景 2：两笔重叠，和 ≤ 在架余量 → 两笔都成 ----------

def test_fits_both_confirms_succeed(fresh_db):
    SCN = "fits_both_confirms"
    iid = add_item()
    add_lot(iid, 5, "2026-11-01")
    base = consumptions_count()
    out = run_parallel(lambda: do_consume(iid, 2), lambda: do_consume(iid, 2))
    assert [t for t, _ in out] == ["ok", "ok"], f"[{SCN}] 两笔都应成功: {out}"
    lots = lots_of(iid)
    assert on_shelf_total(lots) == pytest.approx(1), f"[{SCN}] 余量应为 5-2-2=1: {lots}"
    assert consumptions_count() - base == 2, f"[{SCN}] 应入账两笔"


# ---------- 场景 3：失败路径不得留下半截 qty_remain ----------

def test_short_consume_leaves_no_partial_deduction(fresh_db):
    SCN = "short_no_partial"
    iid = add_item()
    a = add_lot(iid, 2, "2026-10-01")
    b = add_lot(iid, 1, "2026-11-01")
    base = consumptions_count()
    with pytest.raises(ConflictError):
        do_consume(iid, 5)  # 引擎会算出跨两批的部分方案，但服务层必须整笔不扣
    lots = {l["id"]: l for l in lots_of(iid)}
    assert lots[a]["qty_remain"] == 2, f"[{SCN}] 批{a} 被扣了半截: {lots[a]}"
    assert lots[b]["qty_remain"] == 1, f"[{SCN}] 批{b} 被扣了半截: {lots[b]}"
    assert all(l["status"] == "on_shelf" for l in lots.values()), f"[{SCN}] 状态被改动"
    assert consumptions_count() == base, f"[{SCN}] 失败不应入账"


# ---------- 场景 4：第三路 · 入库刚补了一笔更早到期的批 ----------

def test_inbound_earlier_lot_race_reconciles(fresh_db):
    SCN = "inbound_earlier_lot_race"
    iid = add_item()
    add_lot(iid, 2, "2026-12-01")
    base = consumptions_count()
    out = run_parallel(lambda: do_consume(iid, 1),
                       lambda: do_inbound(iid, 4, "2026-01-01"))  # 更早到期
    assert [t for t, _ in out] == ["ok", "ok"], f"[{SCN}] 两路都应成功: {out}"
    lots = lots_of(iid)
    assert len(lots) == 2, f"[{SCN}] 应有两批: {lots}"
    assert on_shelf_total(lots) == pytest.approx(5), f"[{SCN}] 全层余量应为 2+4-1=5: {lots}"
    assert consumptions_count() - base == 1, f"[{SCN}] 应入账一笔"
    assert_deductions_reconcile(f"[{SCN}]", out[0][1], lots)


def test_inbound_then_consume_prefers_earlier_expiry(fresh_db):
    SCN = "inbound_then_consume"
    iid = add_item()
    add_lot(iid, 2, "2026-12-01")
    new = do_inbound(iid, 4, "2026-01-01")["id"]
    res = do_consume(iid, 1)
    assert res["deductions"][0]["lot_id"] == new, f"[{SCN}] 应优先扣更早到期的新批: {res}"


# ---------- 场景 5：第三路 · 下架刚拿走最早那批 ----------

def _sweep_race_setup():
    iid = add_item()
    a = add_lot(iid, 2, "2020-01-01")  # 已过期仍在架：将被下架的最早批
    b = add_lot(iid, 2, "2099-01-01")
    return iid, a, b


def test_consume_then_sweep_keeps_consumed_world(fresh_db):
    SCN = "consume_then_sweep"
    iid, a, b = _sweep_race_setup()
    res = do_consume(iid, 3)
    sweep = do_expire_sweep(TODAY)
    lots = {l["id"]: l for l in lots_of(iid)}
    assert [d["lot_id"] for d in res["deductions"]] == [a, b], f"[{SCN}] FEFO 应先扣最早批"
    assert lots[a]["status"] == "consumed" and lots[a]["qty_remain"] == 0, f"[{SCN}] A 应扣空"
    assert lots[b]["qty_remain"] == 1 and lots[b]["status"] == "on_shelf", f"[{SCN}] B 应余 1"
    assert sweep["expired_ids"] == [], f"[{SCN}] 已消费批不得再被下架: {sweep}"


def test_sweep_then_consume_sees_delisted_world(fresh_db):
    SCN = "sweep_then_consume"
    iid, a, b = _sweep_race_setup()
    sweep = do_expire_sweep(TODAY)
    assert sweep["expired_ids"] == [a], f"[{SCN}] 最早批应被下架: {sweep}"
    base = consumptions_count()
    with pytest.raises(ConflictError) as ei:
        do_consume(iid, 3)
    assert ei.value.result["short"] == pytest.approx(1), f"[{SCN}] 缺口应为 1"
    lots = {l["id"]: l for l in lots_of(iid)}
    assert lots[a]["status"] == "expired" and lots[a]["qty_remain"] == 2, f"[{SCN}] A 不应被扣"
    assert lots[b]["qty_remain"] == 2, f"[{SCN}] B 不应被扣"
    assert consumptions_count() == base, f"[{SCN}] 失败不应入账"


def test_sweep_consume_race_leaves_exactly_one_world(fresh_db):
    SCN = "sweep_consume_race"
    for rnd in range(ROUNDS):
        iid, a, b = _sweep_race_setup()
        base = consumptions_count()
        out = run_parallel(lambda: do_consume(iid, 3), lambda: do_expire_sweep(TODAY))
        lots = {l["id"]: l for l in lots_of(iid)}
        tag = f"[{SCN}#{rnd}]"
        assert all(l["qty_remain"] >= 0 for l in lots.values()), f"{tag} 出现负余量"
        if out[0][0] == "ok":  # 世界 1：consume 先拿锁
            res = out[0][1]
            assert [d["lot_id"] for d in res["deductions"]] == [a, b], f"{tag} 世界1 deductions"
            assert lots[a]["status"] == "consumed" and lots[a]["qty_remain"] == 0, f"{tag} 世界1 A"
            assert lots[b]["qty_remain"] == 1, f"{tag} 世界1 B"
            assert consumptions_count() - base == 1, f"{tag} 世界1 应入账一笔"
        else:  # 世界 2：sweep 先拿锁，consume 冲突整笔回滚
            assert isinstance(out[0][1], ConflictError), f"{tag} 只许成功或冲突: {out}"
            assert lots[a]["status"] == "expired" and lots[a]["qty_remain"] == 2, f"{tag} 世界2 A"
            assert lots[b]["qty_remain"] == 2, f"{tag} 世界2 B"
            assert consumptions_count() - base == 0, f"{tag} 世界2 不应入账"


# ---------- 场景 6：HTTP 状态映射（前端只能看到拍板允许的结果态） ----------

def test_http_status_mapping(fresh_db):
    SCN = "http_status_mapping"
    from fastapi import HTTPException
    from app.main import ConsumeIn, consume
    iid = add_item()
    add_lot(iid, 1, "2026-01-01")
    with pytest.raises(HTTPException) as e409:
        consume(ConsumeIn(item_id=iid, qty=5))
    assert e409.value.status_code == 409, f"[{SCN}] 超扣应 409"
    with pytest.raises(HTTPException) as e400:
        consume(ConsumeIn(item_id=iid, qty=0))
    assert e400.value.status_code == 400, f"[{SCN}] 非正数量应 400"
    r = consume(ConsumeIn(item_id=iid, qty=1))
    assert r["ok"] and sum(d["take"] for d in r["deductions"]) == 1, f"[{SCN}] 正常扣减"
