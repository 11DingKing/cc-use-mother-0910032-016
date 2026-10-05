# 排班资源暂占恢复

团体预约高峰期，客服先为学校**暂占讲解员与场地**，等待学校确认。本项目在领域契约之上
提供一个纯 Python 标准库实现的服务端：租约记录组合资源暂占、持有人、到期时间与续约令牌；
确认时比较版本并转换为正式预约；超时扫描、主动取消、部分资源失效、重复确认全部幂等；
多个清理进程并发运行不会重复释放；API 与管理命令可查看占用来源并安全回收孤儿记录。

对应 `domain/contract.json` 的四条不变式：

| 不变式 | 实现方式 |
| --- | --- |
| 组合资源租约 | 一个租约原子占用多个讲解员/场地，任一冲突整体回滚（`store.py` 单事务 + 资源唯一索引） |
| 确认版本比较 | 每次续约 `version` 自增，确认必须同时出示正确令牌与版本号，否则 409 |
| 并发清理互斥 | 所有终结都走 `UPDATE ... WHERE status='HELD'` 条件更新，多进程下每行只有一方认领成功 |
| 孤儿占用恢复 | 超时扫描、按 id 回收、宽限期孤儿扫描三条路径，重复执行安全幂等 |

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/scheduling_lease/`：暂占恢复服务
  - `store.py`：SQLite（WAL）存储与全部原子状态转换；
  - `service.py`：令牌哈希、版本比较、TTL、幂等与部分资源失效的业务逻辑；
  - `api.py`：HTTP API（标准库 `ThreadingHTTPServer`）；
  - `admin.py`：管理命令。
- `tools/check_contract.py`：契约命令行摘要。
- `tools/lease_admin.py`：暂占管理命令包装。
- `tests/`：契约回归 + 租约语义/并发/HTTP/多进程测试。

## 生命周期

```
                 hold (返回一次性 token, version=1)
   学校/客服 ───────────────────────────────► HELD（暂占中，占容量）
      │ renew（可调整组合资源，version+1）      │
      │◄─────────────────────────────────────┤
      │ confirm(token, version)               │ sweep 超时扫描
      ▼                                        ├─► EXPIRED
CONFIRMED（正式预约，继续占容量）                │ cancel 持有人/管理员
                                               ├─► CANCELLED / RECLAIMED
                                               │ invalidate 讲解员请假/场地维修
                                               └─► INVALIDATED
                        （非正常终结：组合资源快照留档后释放容量）
```

- **HELD** 与 **CONFIRMED** 的占用参与冲突检测；EXPIRED/CANCELLED/RECLAIMED/
  INVALIDATED 的占用行会在同事务内快照到租约后删除，容量立即恢复且有审计留痕。
- 对已终结租约重复确认/取消/回收均返回同一最终状态（幂等），不产生第二条预约或重复释放。

## 启动服务

```bash
export PYTHONPATH=src
export SCHEDULING_ADMIN_TOKEN=请改成强口令   # 管理接口的 X-Admin-Token
export LEASE_DB=./data/scheduling.sqlite3
python3 -m scheduling_lease.api --host 0.0.0.0 --port 8080
```

## HTTP API

持有人接口（令牌在请求体中）：

| 方法 路径 | 说明 |
| --- | --- |
| `POST /v1/leases` | 暂占组合资源，返回 `lease_id`、`token`（仅本次返回）、`version`、到期时间 |
| `GET  /v1/leases?status=HELD` | 查看占用（含来源 `source`、经手人 `created_by`） |
| `GET  /v1/leases/{id}` | 查看单条暂占/终结记录 |
| `POST /v1/leases/{id}/renew` | 续约，可带新 `resources` 调整组合；版本自增 |
| `POST /v1/leases/{id}/confirm` | 凭 `token`+`version` 转正式预约；重复确认 200 且 `idempotent=true` |
| `POST /v1/leases/{id}/cancel` | 持有人主动取消（重复取消幂等） |
| `GET  /v1/bookings/{id}` | 查询正式预约 |

管理接口（需请求头 `X-Admin-Token`）：

| 方法 路径 | 说明 |
| --- | --- |
| `POST /v1/admin/sweep` | 超时扫描一轮，返回本轮实际释放的租约 |
| `POST /v1/admin/reclaim` | 回收孤儿：`lease_ids` 按 id 回收，或 `grace_seconds` 宽限扫描 |
| `POST /v1/admin/leases/{id}/reclaim` | 强制回收单条暂占 |
| `POST /v1/admin/invalid-resources` | 标记部分资源失效并释放受影响暂占（重复登记幂等） |
| `GET  /v1/admin/invalid-resources` | 查看失效资源清单 |

示例：

```bash
curl -XPOST localhost:8080/v1/leases -H 'Content-Type: application/json' -d '{
  "school_id":"实验一小","contact":"王老师",
  "source":"客服电话#66","created_by":"客服-小李","ttl_seconds":900,
  "resources":[
    {"resource_type":"guide","resource_ref":"g-007",
     "slot_start":"2026-10-12T09:00:00+00:00","slot_end":"2026-10-12T10:30:00+00:00"},
    {"resource_type":"venue","resource_ref":"hall-a",
     "slot_start":"2026-10-12T09:00:00+00:00","slot_end":"2026-10-12T10:30:00+00:00"}]}'
# -> {"lease_id":"lease-...","token":"...","version":1,...}

curl -XPOST localhost:8080/v1/leases/lease-.../confirm \
  -H 'Content-Type: application/json' \
  -d '{"token":"...","version":1,"booking_id":"BK-1001"}'
```

冲突响应会直接给出占用来源，方便客服向学校解释：

```json
{"error":"conflict","conflicts":[{"school_id":"实验一小","source":"客服电话#66", ...}]}
```

## 管理命令

```bash
export LEASE_DB=./data/scheduling.sqlite3
python3 tools/lease_admin.py list                 # 占用总览（来源/经手/状态/释放原因）
python3 tools/lease_admin.py list --status HELD   # 只看生效暂占
python3 tools/lease_admin.py show <lease_id>      # 单条详情与释放审计
python3 tools/lease_admin.py sweep                # 超时扫描一轮
python3 tools/lease_admin.py reclaim --id id1 id2 # 安全回收指定孤儿
python3 tools/lease_admin.py reclaim --grace 300  # 回收过期超过 5 分钟的暂占
python3 tools/lease_admin.py cancel <lease_id>    # 管理员强制取消
python3 tools/lease_admin.py invalidate guide:g-007 --reason 讲解员请假
python3 tools/lease_admin.py list-invalid
```

## 并发与正确性要点

- 所有状态转换在单个 `BEGIN IMMEDIATE` 事务内完成（SQLite WAL），终态条件更新是互斥点；
  `tests/test_api.py` 用 6 个真实操作系统进程同时扫描 30 条过期租约，断言每条恰好释放一次。
- 续约/换组与确认互斥：版本比较失败的确认不会转预约；确认与取消并发时只有一方胜出。
- 令牌仅以 SHA-256 摘要落库；管理口令用 `secrets.compare_digest` 常量时间比较。

## 验证

```bash
python3 -m unittest discover -s tests -v     # 19 个测试：契约+语义+并发+多进程+HTTP
python3 -m compileall -q src tools tests
python3 tools/check_contract.py domain/contract.json
```
