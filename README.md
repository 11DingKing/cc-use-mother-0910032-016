# 排班资源暂占恢复

团体预约高峰期，客服先为学校暂占讲解员与场地等待确认；进程异常后这些占用
若长期不释放，其他学校看到的容量就会失真。本项目在领域契约之上构建了一个
**仅依赖 Python 标准库（SQLite 原子状态机）** 的服务端：以租约记录组合资源
暂占、持有人、到期时间与续约令牌，确认时比较版本并转换为正式预约，并保证
超时扫描、主动取消、部分资源失效与重复确认的幂等，多个清理进程不会重复释放。

## 状态机

```
        hold()                    confirm(version + token, confirmation_id)
  ┌──────────────┐  HELD ───────────────────────────────────────▶ CONFIRMED
  │  资源槽位互斥  │    │
  └──────────────┘    ├── cancel(holder token) ─────▶ RELEASED
                      ├── invalidate resource ─────▶ RELEASED（全部失效）/ 保留（部分失效）
                      └── sweep() 两阶段认领+释放 ──▶ EXPIRED
```

- `HELD` 暂占中，占资源槽位；`CONFIRMED` 正式预约，继续占槽位。
- `RELEASED` / `EXPIRED` 终态：资源行置 `RELEASED`/`INVALID`，腾出容量。
- 每个 `(resource_type, resource_id, slot)` 上有**部分唯一索引**
  `WHERE state='ACTIVE'`：同一时刻同一槽位只允许一条占用（暂占或正式预约）。

## 目录

- `domain/contract.json`：领域角色、状态、约束（组合资源租约/确认版本比较/
  并发清理互斥/孤儿占用恢复）和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/schedule_leases/`：
  - `store.py`：SQLite 原子状态机（条件 UPDATE = CAS、部分唯一索引、扫描认领）。
  - `service.py`：租约业务逻辑（暂占/续约/确认/取消/失效/清理/管理回收）。
  - `server.py`：HTTP API（`http.server`，可挂后台扫描线程）。
  - `cli.py` / `__main__.py`：管理命令。
  - `clock.py`：测试用可控时钟。
- `tools/check_contract.py`：命令行摘要检查。
- `tests/`：契约完整性 + 租约幂等/版本/多进程互斥回归测试。

## 关键设计：四个不变量如何保证

### 1. 组合资源租约
一个 lease 聚合多类资源（讲解员 `guide` + 场地 `venue` + 时间槽 `slot`），
记录 `holder`（持有人/客服）、`holder_kind`、`expires_at`、`renew_token`、
`expected_capacity_version`。槽位由 `lease_resource` 表的部分唯一索引互斥。

### 2. 确认版本比较
确认必须同时提供 `renew_token` 与 `expected_version`。续约/每次变更令
`version + 1`；学校看容量时拿到的版本若在确认前被续约/改动过，CAS 因
`version != expected_version` 失败（`version_conflict`，409），要求重新读取。
`confirmation_id` 是调用方幂等键：网络重试重复确认同键只转换一次，且不能跨租约复用。

### 3. 并发清理互斥
超时回收采用**两阶段**：
1. **认领**：`UPDATE ... WHERE state='HELD' AND expires_at<=:now AND
   reclaim_token IS NULL` 原子写上本进程的 worker 令牌；
2. **释放**：仅当 `reclaim_token == 本进程` 时才 CAS 释放为 EXPIRED。

多个清理进程（多连接/多线程/**多 OS 进程**）并发时，每条记录只可能被一个
进程认领并释放。清理者崩溃遗留的认领，超过 5 分钟宽限期后由其他进程
**接管（reclaim stale claims）**；宽限期内不动，避免与尚在运行的清理者竞争。

### 4. 孤儿占用恢复
- `admin release <id> --reason ...`：安全回收 HELD 暂占，对已终结记录幂等；
  **CONFIRMED 正式预约拒绝回收**（必须走取消预约流程，不能静默吞掉正式预约）。
- 资源失效分两种：全部失效则租约整体 RELEASED；**部分失效**则租约保留、
  失效槽位立即腾出（其他学校可立即预订），客服再重配。CONFIRMED 不被静默失效。
- `list` / `show` 可查看**占用来源**（holder、holder_kind、创建时间、
  回收者、回收原因）与完整事件流水（HOLD/RENEW/CONFIRM/RELEASE/EXPIRE/
  RESOURCE_INVALID）。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/leases` | 暂占组合资源 |
| GET  | `/leases/{id}` | 查看占用（含 `source` 来源与 `events` 流水） |
| GET  | `/leases?state=&holder=&all=` | 列表 |
| POST | `/leases/{id}/renew` | 续约（version+1） |
| POST | `/leases/{id}/confirm` | 比较版本确认（`confirmation_id` 幂等） |
| POST | `/leases/{id}/cancel` | 持有人主动取消（幂等） |
| POST | `/capacity` | 槽位占用查询（排查容量失真） |
| POST | `/admin/invalidate` | 声明资源槽位失效 |
| POST | `/admin/sweep` | 触发一轮超时回收 |
| POST | `/admin/leases/{id}/release` | 安全回收孤儿记录 |

## 管理命令

```bash
# 启动 API（--sweep-interval 可顺带跑后台扫描；也可由 cron 多进程跑 sweep）
python3 -m schedule_leases --db leases.db serve --port 8080 --sweep-interval 30

# 查看占用来源（默认隐藏已终结）
python3 -m schedule_leases --db leases.db list --state HELD
python3 -m schedule_leases --db leases.db show <lease_id>

# 多进程并发安全的超时回收
python3 -m schedule_leases --db leases.db sweep --worker-id cron-node-1

# 安全回收孤儿（--reason 必填审计；不会动正式预约）
python3 -m schedule_leases --db leases.db release <lease_id> --reason "进程崩溃遗留"

# 资源失效 / 容量查询
python3 -m schedule_leases --db leases.db invalidate --type guide --id g-07 --slot 2026-10-20T09:00
echo '[{"resource_type":"venue","resource_id":"v-02","slot":"2026-10-20T09:00"}]' \
  | python3 -m schedule_leases --db leases.db capacity
```

## 验证

```bash
python3 -m unittest discover -s tests -v     # 19 个回归测试
python3 -m compileall -q src tools tests     # 编译检查
python3 tools/check_contract.py domain/contract.json
```

已实测：6 个真实 OS 进程并发扫描 20 条到期占用，恰有一个进程释放全部 20 条，
其余释放 0 条，每条记录只有一个 EXPIRE 事件；再次扫描为 0（幂等）。
