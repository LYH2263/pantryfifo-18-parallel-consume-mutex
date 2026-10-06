"""并行扣减互斥测例。

用真实 uvicorn（同步端点跑在线程池）+ 多线程复现请求重叠，通过 testing_gate
让两路请求同时在飞，并指定谁先拿写锁（先到者拍板，留下它的世界）。

每个场景结束都核对三件事必须指向同一种世界：
  1. 成功响应里的 deductions / world.lot_remain_after
  2. 数据库 lots 真实状态（无半截、无负数 qty_remain）
  3. /api/fridge 层页聚合余量 == world.layer_remain_after
任何一条不过，打印场景名后失败。
"""
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

BACKEND = Path(__file__).resolve().parents[2]
PORT = 19091
BASE = f"http://127.0.0.1:{PORT}"
ITEM = 100  # 专用并发测试品项（上层），与 seed 数据隔离

# ---------------------------------------------------------------- 服务器夹具

@pytest.fixture(scope="session")
def server():
    data = tempfile.mkdtemp(prefix="pf-conc-")
    log = open(Path(data) / "srv.log", "w")
    env = {**os.environ, "PF_TEST_GATE": "1", "DATA_DIR": data,
           "PYTHONPATH": str(BACKEND)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(PORT)],
        cwd=str(BACKEND), env=env, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            if httpx.get(BASE + "/api/health", timeout=1).status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.15)
    else:
        log.flush()
        raise RuntimeError("server failed to start\n" + Path(data).joinpath("srv.log").read_text())
    yield {"proc": proc, "data": data, "log": Path(data) / "srv.log"}
    proc.terminate()
    proc.wait(timeout=10)
    log.close()

# ---------------------------------------------------------------- 工具函数

def dbfile(server):
    return Path(server["data"]) / "pantryfifo.db"

def db():
    # data dir 由夹具间接传入：通过环境变量在每个测试前设置不方便，
    # 直接读取固定进程参数 —— 用模块级变量记录。
    return _CONN[0]

_CONN = {}

@pytest.fixture(autouse=True)
def _wire_db(server):
    c = sqlite3.connect(dbfile(server), timeout=10)
    c.execute("PRAGMA busy_timeout=10000")
    _CONN[0] = c
    c.execute(
        "INSERT OR IGNORE INTO items(id,name,layer,unit) VALUES (?,?,?,?)",
        (ITEM, "并发测试奶", "upper", "盒"))
    # 把 seed 自带批次全部请下货架，使层页余量完全由本场景决定
    c.execute("UPDATE lots SET status='seed_stashed' WHERE item_id<>?", (ITEM,))
    c.execute("DELETE FROM lots WHERE item_id=?", (ITEM,))
    c.execute("DELETE FROM consumptions")
    c.execute("DELETE FROM consume_tokens")
    c.commit()
    yield
    c.close()

def seed_lots(specs):
    """specs: [(qty_remain, expiry), ...]"""
    c = db()
    for qty, exp in specs:
        c.execute(
            "INSERT INTO lots(item_id,qty_in,qty_remain,expiry,status,data_quality)"
            " VALUES (?,?,?,?,?,?)",
            (ITEM, qty, qty, exp, "on_shelf", "clean"))
    c.commit()

def db_lots():
    rows = db().execute(
        "SELECT id,expiry,qty_remain,status FROM lots WHERE item_id=? ORDER BY expiry,id",
        (ITEM,)).fetchall()
    return {r[0]: {"expiry": r[1], "qty_remain": round(float(r[2]), 3), "status": r[3]}
            for r in rows}

def db_count(table):
    return db().execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

def fridge_layer_map():
    rows = httpx.get(BASE + "/api/fridge", timeout=5).json()
    agg: dict[str, float] = {}
    for r in rows:
        agg[r["layer"]] = round(agg.get(r["layer"], 0.0) + float(r["qty_remain"]), 3)
    return {k: v for k, v in agg.items() if v > 1e-9}

def call(label, method, path, payload=None):
    r = httpx.request(method, BASE + path, json=payload, timeout=15)
    try:
        body = r.json()
    except Exception:
        body = r.text
    return label, r.status_code, body

def run_overlap(title, parties, blockers, calls):
    """calls: [{label, method, path, json}]。两路同时发出，blockers 里的一方
    主动让出写锁 0.4s，另一方先拍板。"""
    httpx.post(BASE + "/__gate/reset", timeout=5)
    httpx.post(BASE + "/__gate/config", json={
        "scenario": title, "parties": parties, "block": blockers}, timeout=5)
    out = {}
    with ThreadPoolExecutor(max_workers=len(calls)) as pool:
        futs = [pool.submit(call, c["label"], c["method"], c["path"], c.get("json"))
                for c in calls]
        deadline = time.time() + 15
        want = set(parties)
        while time.time() < deadline:
            arrived = set(httpx.get(BASE + "/__gate/arrived", timeout=5).json()["arrived"])
            if want.issubset(arrived):
                break
            time.sleep(0.02)
        else:
            raise AssertionError(f"【{title}】请求未全部到达 gate: {arrived}")
        time.sleep(0.05)  # 让落后的一方也堵在锁前
        httpx.post(BASE + "/__gate/release", timeout=5)
        for f in futs:
            label, status, body = f.result(timeout=20)
            out[label] = (status, body)
    httpx.post(BASE + "/__gate/reset", timeout=5)
    return out

def consume_call(tag, qty, token):
    return {"label": f"consume:{tag}", "method": "POST", "path": "/api/consume",
            "json": {"item_id": ITEM, "qty": qty, "token": token, "gate_tag": tag}}

def inbound_call(tag, qty, expiry):
    return {"label": f"inbound:{tag}", "method": "POST", "path": "/api/lots",
            "json": {"item_id": ITEM, "qty": qty, "expiry": expiry, "gate_tag": tag}}

SWEEP = {"label": "sweep:s", "method": "POST", "path": "/api/expire-sweep?gate_tag=s"}

def detail(body):
    return body["detail"] if isinstance(body, dict) and "detail" in body else body

def finish(title, errs, server=None):
    if errs:
        print(f"\n❌❌ 场景失败【{title}】")
        for e in errs:
            print("   -", e)
        if server and server["log"].exists():
            tail = server["log"].read_text().splitlines()[-15:]
            print("   --- 服务日志尾部 ---")
            print("\n".join("   " + l for l in tail))
        pytest.fail(f"场景【{title}】出现 {len(errs)} 条失败")
    print(f"✅ 场景通过【{title}】")

def ded_sig(body):
    return [(d["expiry"], round(float(d["take"]), 3)) for d in body["deductions"]]

def assert_world_matches_shelf(title, errs, world, lots):
    """响应里的世界 == DB 批次余量 == 层页聚合。"""
    wlots = world["lot_remain_after"]
    for lid, info in lots.items():
        expect_rem = 0.0 if info["status"] in ("consumed", "expired") else info["qty_remain"]
        got = float(wlots.get(str(lid), 0.0))
        if abs(got - expect_rem) > 1e-9:
            errs.append(f"lot {lid} 世界余量 {got} 与 DB {expect_rem}({info['status']}) 不一致")
    item_total = round(sum(
        v["qty_remain"] for v in lots.values() if v["status"] == "on_shelf"), 3)
    if abs(float(world["item_remain_after"]) - item_total) > 1e-9:
        errs.append(f"item_remain_after={world['item_remain_after']} 与 DB 在架合计 {item_total} 不一致")
    fridge = fridge_layer_map()
    if world.get("layer_remain_after") != fridge:
        errs.append(f"全层余量 {world.get('layer_remain_after')} 与层页 /api/fridge 聚合 {fridge} 不一致")

def no_negative_qty(errs):
    for lid, info in db_lots().items():
        if info["qty_remain"] < -1e-9:
            errs.append(f"lot {lid} 出现负余量 {info['qty_remain']}：失败路径留下了半截扣减")

# ---------------------------------------------------------------- 场景

def test_overlap_overbook_first_wins(server):
    """两笔确认重叠且数量之和(8) > 在架正余量(5)：先到者拍板成功，后到者
    按新世界重算后不足 → 整体 409 回滚，不留半截。"""
    title = "重叠超量·先到拍板后到失败"
    seed_lots([(3, "2026-11-01"), (2, "2026-12-01")])
    calls = [consume_call("first", 4, "s1-a"), consume_call("second", 4, "s1-b")]
    res = run_overlap(title, [c["label"] for c in calls], ["consume:second"], calls)
    errs = []
    (s1, b1), (s2, b2) = res["consume:first"], res["consume:second"]
    if s1 != 200 or s2 != 409:
        errs.append(f"应先到 200/后到 409，实际 {s1}/{s2}")
    if s1 == 200:
        if ded_sig(b1) != [("2026-11-01", 3.0), ("2026-12-01", 1.0)]:
            errs.append(f"拍板者 deductions 错误: {ded_sig(b1)}")
        assert_world_matches_shelf(title, errs, b1["world"], db_lots())
    if s2 == 409:
        d = detail(b2)
        if d.get("reason") != "short" or abs(float(d.get("short", 0)) - 3) > 1e-9:
            errs.append(f"后到者应 short=3，实际 reason={d.get('reason')} short={d.get('short')}")
        obs = [(o["expiry"], o["qty_remain"]) for o in d.get("observed_lots", [])]
        if obs != [("2026-12-01", 1.0)]:
            # first 已把早批吃光、晚批吃掉 1；后到者只能看见剩 1 的晚批
            errs.append(f"后到者观察到的世界应为仅剩晚批1，实际 {obs}")
    lots = db_lots()
    statuses = {v["expiry"]: (v["status"], v["qty_remain"]) for v in lots.values()}
    if statuses.get("2026-11-01") != ("consumed", 0.0):
        errs.append(f"早批应为 consumed/0，实际 {statuses.get('2026-11-01')}")
    if statuses.get("2026-12-01") != ("on_shelf", 1.0):
        errs.append(f"晚批应剩 1，实际 {statuses.get('2026-12-01')}")
    if db_count("consumptions") != 1:
        errs.append(f"只应留下一笔成功扣减记录，实际 {db_count('consumptions')}")
    no_negative_qty(errs)
    finish(title, errs, server)

def test_overlap_overbook_lock_order_flips(server):
    """同一个超量重叠，让出锁的一方换成 first：赢家随之翻转，
    最终世界仍然只有一种、且与页面对齐。"""
    title = "重叠超量·锁序翻转赢家翻转"
    seed_lots([(3, "2026-11-01"), (2, "2026-12-01")])
    calls = [consume_call("first", 4, "s2-a"), consume_call("second", 4, "s2-b")]
    res = run_overlap(title, [c["label"] for c in calls], ["consume:first"], calls)
    errs = []
    (s1, _b1), (s2, b2) = res["consume:first"], res["consume:second"]
    if s1 != 409 or s2 != 200:
        errs.append(f"翻转后应 first 409/second 200，实际 {s1}/{s2}")
    if s2 == 200:
        if ded_sig(b2) != [("2026-11-01", 3.0), ("2026-12-01", 1.0)]:
            errs.append(f"拍板者 deductions 错误: {ded_sig(b2)}")
        assert_world_matches_shelf(title, errs, b2["world"], db_lots())
    no_negative_qty(errs)
    if db_count("consumptions") != 1:
        errs.append("只应有一笔成功")
    finish(title, errs, server)

def test_overlap_exact_both_commit(server):
    """两笔数量之和恰好等于在架正余量：两笔都成功，但 FEFO 批次划分互相对齐，
    合起来恰好把在架量吃光，没有双花。"""
    title = "重叠恰够·两笔都拍板成功"
    seed_lots([(3, "2026-11-01"), (2, "2026-12-01")])
    calls = [consume_call("first", 3, "s3-a"), consume_call("second", 2, "s3-b")]
    res = run_overlap(title, [c["label"] for c in calls], ["consume:second"], calls)
    errs = []
    (s1, b1), (s2, b2) = res["consume:first"], res["consume:second"]
    if (s1, s2) != (200, 200):
        errs.append(f"两笔都应 200，实际 {s1}/{s2}")
    if s1 == 200 and ded_sig(b1) != [("2026-11-01", 3.0)]:
        errs.append(f"先到者应独拿早批 3，实际 {ded_sig(b1)}")
    if s2 == 200:
        if ded_sig(b2) != [("2026-12-01", 2.0)]:
            errs.append(f"后到者在新世界里只能拿晚批 2，实际 {ded_sig(b2)}")
        assert_world_matches_shelf(title, errs, b2["world"], db_lots())
    if any(v["status"] != "consumed" or v["qty_remain"] != 0 for v in db_lots().values()):
        errs.append(f"两批都应耗尽 consumed/0，实际 {db_lots()}")
    if db_count("consumptions") != 2:
        errs.append("应有两笔成功记录")
    no_negative_qty(errs)
    finish(title, errs, server)

def test_third_road_inbound_earlier_lot_first(server):
    """第三路 A：入库先提交一笔更早到期的批。消费等待后按含新批的世界
    FEFO：先吃新批 2，再吃旧批 1，旧批留 2。"""
    title = "第三路·入库更早批先拍板"
    seed_lots([(3, "2030-01-01")])
    calls = [consume_call("c", 3, "s4-c"), inbound_call("i", 2, "2026-10-20")]
    res = run_overlap(title, [c["label"] for c in calls], ["consume:c"], calls)
    errs = []
    sc, bc = res["consume:c"]
    si, bi = res["inbound:i"]
    if (sc, si) != (200, 200):
        errs.append(f"入库与消费都应成功，实际 {si}/{sc}")
    if sc == 200:
        if ded_sig(bc) != [("2026-10-20", 2.0), ("2030-01-01", 1.0)]:
            errs.append(f"应先扣新到的更早批再扣旧批，实际 {ded_sig(bc)}")
        obs = sorted(o["expiry"] for o in bc.get("observed_lots", []))
        if obs != ["2026-10-20", "2030-01-01"]:
            errs.append(f"消费观察到的世界应包含更早新批，实际 {obs}")
        assert_world_matches_shelf(title, errs, bc["world"], db_lots())
    final = {v["expiry"]: (v["status"], v["qty_remain"]) for v in db_lots().values()}
    if final.get("2026-10-20") != ("consumed", 0.0):
        errs.append(f"新批应耗尽，实际 {final.get('2026-10-20')}")
    if final.get("2030-01-01") != ("on_shelf", 2.0):
        errs.append(f"旧批应剩 2，实际 {final.get('2030-01-01')}")
    no_negative_qty(errs)
    finish(title, errs, server)

def test_third_road_consume_first_then_inbound(server):
    """第三路 B：消费先拍板（在架只有 3，要 4 → 失败回滚），入库随后补一笔
    更早批。失败消费不得改动任何 qty_remain；最终世界只含入库这一种变化。"""
    title = "第三路·消费先失败入库后补批"
    seed_lots([(3, "2030-01-01")])
    calls = [consume_call("c", 4, "s5-c"), inbound_call("i", 2, "2026-10-20")]
    res = run_overlap(title, [c["label"] for c in calls], ["inbound:i"], calls)
    errs = []
    sc, bc = res["consume:c"]
    si, _bi = res["inbound:i"]
    if sc != 409 or si != 200:
        errs.append(f"应消费 409/入库 200，实际 {sc}/{si}")
    if sc == 409:
        d = detail(bc)
        if d.get("reason") != "short" or abs(float(d.get("short", 0)) - 1) > 1e-9:
            errs.append(f"消费应 short=1，实际 {d.get('reason')}/{d.get('short')}")
        obs = [(o["expiry"], o["qty_remain"]) for o in d.get("observed_lots", [])]
        if obs != [("2030-01-01", 3.0)]:
            errs.append(f"失败世界里应只看见旧批 3，实际 {obs}")
        # 失败响应携带的世界快照：item 余量 3，未被入库污染、也未被自己扣掉
        if float(d["world"]["item_remain_after"]) != 3.0:
            errs.append("失败响应世界快照余量应为 3")
    final = {v["expiry"]: (v["status"], v["qty_remain"]) for v in db_lots().values()}
    if final.get("2030-01-01") != ("on_shelf", 3.0):
        errs.append(f"失败回滚后旧批必须原封不动仍为 3，实际 {final.get('2030-01-01')}")
    if final.get("2026-10-20") != ("on_shelf", 2.0):
        errs.append(f"入库新批应在架 2，实际 {final.get('2026-10-20')}")
    if db_count("consumptions") != 0:
        errs.append("失败消费不得写 consumptions")
    # 最终层页只反映入库后的唯一世界：上层合计 5
    if fridge_layer_map().get("upper") != 5.0:
        errs.append(f"层页上层应合计 5，实际 {fridge_layer_map()}")
    no_negative_qty(errs)
    finish(title, errs, server)

def test_third_road_sweep_takes_earliest_first(server):
    """第三路 C：过期下架先拿走最早那批，消费只能在剩下的晚批上 FEFO。"""
    title = "第三路·下架先拿走最早批"
    seed_lots([(2, "2020-01-01"), (3, "2030-01-01")])
    calls = [SWEEP, consume_call("c", 3, "s6-c")]
    res = run_overlap(title, [c["label"] for c in calls], ["consume:c"], calls)
    errs = []
    ss, bs = res["sweep:s"]
    sc, bc = res["consume:c"]
    if (ss, sc) != (200, 200):
        errs.append(f"下架与消费都应成功，实际 {ss}/{sc}")
    if sc == 200:
        obs = [o["expiry"] for o in bc.get("observed_lots", [])]
        if obs != ["2030-01-01"]:
            errs.append(f"早批已下架，消费世界里只能看见晚批，实际 {obs}")
        if ded_sig(bc) != [("2030-01-01", 3.0)]:
            errs.append(f"应独扣晚批 3，实际 {ded_sig(bc)}")
        assert_world_matches_shelf(title, errs, bc["world"], db_lots())
    final = {v["expiry"]: (v["status"], v["qty_remain"]) for v in db_lots().values()}
    if final.get("2020-01-01") != ("expired", 2.0):
        errs.append(f"早批应原样下架 expired/2，实际 {final.get('2020-01-01')}")
    if final.get("2030-01-01") != ("consumed", 0.0):
        errs.append(f"晚批应耗尽，实际 {final.get('2030-01-01')}")
    no_negative_qty(errs)
    finish(title, errs, server)

def test_third_road_consume_before_sweep(server):
    """第三路 D：消费先拍板——它吃掉了已过期的早批（FEFO 最早），等下架
    运行时早批已是 consumed，不会被二次标记 expired。只留一种世界。"""
    title = "第三路·消费先吃掉早批后下架扑空"
    seed_lots([(2, "2020-01-01"), (3, "2030-01-01")])
    calls = [SWEEP, consume_call("c", 3, "s7-c")]
    res = run_overlap(title, [c["label"] for c in calls], ["sweep:s"], calls)
    errs = []
    ss, bs = res["sweep:s"]
    sc, bc = res["consume:c"]
    if (ss, sc) != (200, 200):
        errs.append(f"两路都应 200，实际 {ss}/{sc}")
    if sc == 200:
        if ded_sig(bc) != [("2020-01-01", 2.0), ("2030-01-01", 1.0)]:
            errs.append(f"消费应先吃早批2再吃晚批1，实际 {ded_sig(bc)}")
        assert_world_matches_shelf(title, errs, bc["world"], db_lots())
    if ss == 200 and bs.get("expired_ids") != []:
        errs.append(f"早批已被消费走，下架应扑空返回 []，实际 {bs.get('expired_ids')}")
    final = {v["expiry"]: (v["status"], v["qty_remain"]) for v in db_lots().values()}
    if final.get("2020-01-01") != ("consumed", 0.0):
        errs.append(f"早批应是 consumed 而非 expired，实际 {final.get('2020-01-01')}")
    if final.get("2030-01-01") != ("on_shelf", 2.0):
        errs.append(f"晚批应剩 2，实际 {final.get('2030-01-01')}")
    no_negative_qty(errs)
    finish(title, errs, server)

def test_idempotent_double_confirm(server):
    """前端一次确认手势双发（同 token）：两路重叠也只扣一次，输家整体回滚后
    重放赢家结果。只能看到一种成功态。"""
    title = "同手势双确认·幂等只扣一次"
    seed_lots([(2, "2026-11-01")])
    calls = [consume_call("a", 2, "gesture-dup"), consume_call("b", 2, "gesture-dup")]
    res = run_overlap(title, [c["label"] for c in calls], ["consume:b"], calls)
    errs = []
    (sa, ba), (sb, bb) = res["consume:a"], res["consume:b"]
    if (sa, sb) != (200, 200):
        errs.append(f"双确认都应得到 200（一次真实+一次重放），实际 {sa}/{sb}")
    replayed = [bool(ba.get("replayed")), bool(bb.get("replayed"))]
    if sum(replayed) != 1:
        errs.append(f"应恰好一路是重放，实际 replayed 标记 {replayed}")
    if ded_sig(ba) != [("2026-11-01", 2.0)] or ded_sig(bb) != [("2026-11-01", 2.0)]:
        errs.append("两路看到的成功态必须是同一种 deductions")
    winner = bb if replayed[1] is False else ba
    assert_world_matches_shelf(title, errs, winner["world"], db_lots())
    if db_count("consume_tokens") != 1 or db_count("consumptions") != 1:
        errs.append(
            f"只应有一条幂等键/一笔扣减，实际 tokens={db_count('consume_tokens')} "
            f"consumptions={db_count('consumptions')}")
    final = {v["expiry"]: (v["status"], v["qty_remain"]) for v in db_lots().values()}
    if final.get("2026-11-01") != ("consumed", 0.0):
        errs.append(f"批次只应被扣一次到 0，实际 {final.get('2026-11-01')}")
    no_negative_qty(errs)
    finish(title, errs, server)

def test_sequential_failure_leaves_no_half(server):
    """无并发的回归：成功一笔后再要一笔超量的，409 不得留下半截 qty_remain，
    世界快照与层页一致。"""
    title = "串行短扣·失败不留半截"
    seed_lots([(3, "2026-11-01"), (2, "2026-12-01")])
    errs = []
    _, s1, b1 = call("x", "POST", "/api/consume",
                     {"item_id": ITEM, "qty": 4, "token": "s9-a"})
    _, s2, b2 = call("x", "POST", "/api/consume",
                     {"item_id": ITEM, "qty": 10, "token": "s9-b"})
    if s1 != 200 or s2 != 409:
        errs.append(f"应 200 后 409，实际 {s1}/{s2}")
    if s2 == 409 and detail(b2).get("reason") != "short":
        errs.append("第二笔应 short")
    final = {v["expiry"]: v["qty_remain"] for v in db_lots().values()}
    if final != {"2026-11-01": 0.0, "2026-12-01": 1.0}:
        errs.append(f"失败后余量应保持 早批0/晚批1，实际 {final}")
    if s1 == 200:
        assert_world_matches_shelf(title, errs, b1["world"], db_lots())
    no_negative_qty(errs)
    finish(title, errs, server)
