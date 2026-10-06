# Pantryfifo · 冰箱临期先吃

分批入库 → FEFO 扣减 → 过期下架。

| 服务 | 端口 |
| --- | --- |
| 前端 | 5300 |
| API | 10300 |

0-1：`shopping_list` / `recipe_suggest` / `temp_zone`。

## 并发扣减拍板：first-wins

同一品项的消费确认、入库、过期下架共用 `BEGIN IMMEDIATE` 临界区
（`app/services.py`）：先拿锁者改 lots 并提交；后到者在锁内基于新世界
重新判定，余量不足即 409 整笔回滚，不留半截 `qty_remain`。
两笔之和超过在架余量时，恰好一笔成功。测例见
`app/tests/test_consume_concurrency.py`（失败打印场景名）。
