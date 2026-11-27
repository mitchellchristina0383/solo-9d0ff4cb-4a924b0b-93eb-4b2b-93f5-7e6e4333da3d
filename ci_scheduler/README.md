# CI Scheduler Drill — FastAPI 单执行器 + 优先级继承 + 等待环复现包

共享工作区被后台构建占着时的调度演练。每个工作区是互斥资源；构建由
`acquire` / `release` / `compile` 步骤组成。系统只有**一个执行器**，每次
`tick` 让**有效优先级最高的就绪构建恰好走一步**。

当一次 `acquire` 触发等待环时，该推进结果会获得复现号；凭号可下载自包含
JSON 包。把下载后的 JSON 发给 `/replay`，服务会在一个全新的隔离调度器演练中
按原规则重放，不影响现场状态。

## 调度规则（已实现）

1. **单执行器**：`POST /tick` 每次只推进一个就绪构建的一个步骤；
   `POST /advance` 可一次推进多步（无就绪构建时提前停止）。
2. **步骤类型**
   - `acquire(workspace)`：工作区空闲则占用；被占用则进入等待。
   - `release(workspace)`：释放自己持有的工作区。
   - `compile(duration_ms)`：模拟一段编译（仅记录时长，不 sleep）。
3. **优先级继承（沿占用关系逐级传递）**
   - 等待者的有效优先级传给持有者，持有者取
     `max(自身基础优先级, 所有收到的优先级)`。
   - 等待关系是森林：构建最多等一个工作区，工作区最多被一个构建持有，
     因此可沿 `申请者 -> 工作区 -> 持有者` 链多级传递。
4. **释放后重新竞争**：工作区释放时，等待队列里有效优先级最高者获得
   （同优先级按提交顺序 `seq` 破平），其余等待者继续等待。
5. **取消**：只允许取消等待中的构建；取消后从等待队列移除，释放它自身
   持有的全部工作区，并仅对受影响的等待链重算优先级。
6. **等待环检测与环路径**：`acquire` 若会使
   `持有者 -> 等待者 -> 持有者……` 链回到申请者，则申请者立即失败
   （`status=failed`，带 `cycle: true`、`cycle_path` 和完整 `cycle_ring`），
   归还它持有的工作区，等待者随之被唤醒重新竞争。环路径采用交替的
   `构建, 工作区, 构建, ...` 表示，例如
   `["A","ws-2","B","ws-1","A"]`。
7. **结束归还**：构建执行完最后一步时释放它占用的全部工作区。若最后一个
   步骤是之前挂起、之后才被授予的 `acquire`，授予时立即收尾，不会留下
   `pc` 越界的 ready 构建。
8. **增量重算**：优先级只沿受影响的等待链向上重算，链上某节点值不变即
   提前终止；测试用“从零固定点全量重算”在随机交织下校验二者始终一致。
9. **复现操作记录**：每轮从 `/reset` 指定的初始工作区开始，按互斥锁内
   实际生效顺序记录成功的工作区创建、构建提交、取消和每一拍 tick。
   `/advance` 只是客户端循环，每拍独立取锁；其他 HTTP 请求在两拍之间插入时，
   会按真实顺序进入日志。

## 复现号与 JSON 包

触发等待环的 tick/advance 结果包含：

```json
{
  "cycle": true,
  "reproduction_id": "repro-000001",
  "repro_id": "repro-000001",
  "replay_id": "repro-000001",
  "cycle_path": ["A", "ws-2", "B", "ws-1", "A"]
}
```

下载：

```bash
curl -OJ http://localhost:8000/reproductions/repro-000001/package
```

包内主要字段：

- `initial_workspaces` / `initial_workspace_ids`：本轮初始工作区；
- `operations`（同时提供别名 `journal` / `timeline`）：按实际生效顺序排列的
  成功操作；每个 tick 的 `result` 保存当时的推进结果；
- `workspace_creations`、`build_submissions`、`cancellations`、`ticks`
  是同一操作记录的便捷投影；
- `failed_build_id`、`failed_build`：报环后失败构建的快照；
- `cycle_path`、`cycle_ring`、`cycle_build_ids`、`cycle_workspace_ids`：
  等待环路径和结构化节点；
- `failure` / `cycle_report` / `last_tick`：故障拍的完整核对基准。

同轮多次报环共用同一份操作日志；每个复现号只保存自己的故障拍序号，因此
各自下载到的包截止到自己的报环拍。`/reset` 会清空当前复现号表，旧号再访问
返回 404；但已经下载到客户端的 JSON 包仍然自包含、可重放。

## 重放 API

```bash
curl -X POST http://localhost:8000/replay \
  -H 'Content-Type: application/json' \
  --data-binary @repro-000001.json
```

重放器会：

1. 用包内初始工作区创建一个全新的 `Scheduler`；
2. 按 `operations` 原顺序执行创建、提交、取消和 tick/advance；
3. 核对最后一拍的 tick 号、失败构建、工作区、环路径及完整结果字段；
4. 返回 `matched`、`failed_build_id`、`cycle_path`、`cycle_ring`、
   `last_tick`，以及隔离演练自己的 `snapshot` 和 `events`。

默认情况下，无法复现返回 HTTP 200 且 JSON 中
`matched=false`、`status="mismatch"`、`mismatches=[...]`；如果调用方希望把
不匹配当作 HTTP 错误，可使用 `POST /replay?strict=true`，此时返回 409。
也支持把包放在 `{"package": {...}}` 信封中提交。

重放调度器是请求内新建的独立对象，不使用模块级单例；重放产生的状态和事件
不会进入现场 `/snapshot` 或 `/events`。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workspaces` | 新建工作区（失败请求不入复现日志） |
| GET | `/workspaces` | 工作区占用者与等待队列 |
| POST | `/builds` | 提交构建（基础优先级 + 步骤列表） |
| GET | `/builds` | 全部构建（含有效优先级、等待对象） |
| GET | `/builds/{id}` | 单个构建详情 |
| POST | `/builds/{id}/cancel` | 取消等待中的构建 |
| POST | `/tick` | 单执行器推进一步；报环时附复现号 |
| POST | `/advance` | 推进多步；报环的那一项附复现号 |
| GET | `/reproductions` | 当前轮可下载的报环记录 |
| GET | `/reproductions/{id}` | 复现包 JSON（默认带下载头） |
| GET | `/reproductions/{id}/package` | 同上，语义化下载路径 |
| POST | `/reproductions/{id}/replay` | 用当前轮复现号重放 |
| POST | `/replay` | 重放下载的 JSON 包，隔离演练 |
| GET | `/journal` | 当前轮成功操作的实际顺序 |
| GET | `/events?limit=n` | 每步结果与全部状态变更事件 |
| GET | `/snapshot` | 工作区 + 构建完整快照 |
| POST | `/reset` | 重置工作区集合、操作日志和复现号 |

为兼容常见命名，还注册了 `/repro/{id}/package`、`/repros/{id}/package`、
`/reproduction-packages/{id}/package`、`/replay-package`、`/api/replay`
等隐藏别名（不在 Swagger 主列表中展开）。

启动后交互式文档：<http://localhost:8000/docs>（Swagger UI，可直接试跑）。

## 用 Docker Compose 启动

```bash
cd ci_scheduler
docker compose up --build
# 打开 http://localhost:8000/docs
```

镜像只复制 `app/`，新增的 `app/replay.py` 会随 `COPY app ./app` 进入镜像；
没有新增第三方依赖。

## 本地（无 Docker 时）开发与测试

```bash
cd ci_scheduler
python3 -m pip install --target=../.pylibs -r requirements.txt pytest httpx
PYTHONPATH=../.pylibs python3 -m pytest tests/ -q
PYTHONPATH=../.pylibs python3 -m uvicorn app.main:app --port 8000
PYTHONPATH=../.pylibs python3 scripts/demo.py
PYTHONPATH=../.pylibs python3 scripts/repro_demo.py \
  http://127.0.0.1:8000 /tmp/ci-repro-demo
```

`scripts/repro_demo.py` 会通过真实 HTTP：

- 发起 10,000 拍的 `/advance`，同时用独立客户端插入 40 个工作区创建请求，
  并断言至少有插入操作落在两拍之间；
- 制造两节点等待环、下载 JSON 包到指定目录；
- 重放成功后核对现场快照不变；
- `/reset` 后验证旧复现号返回 404、已保存 JSON 仍可重放；
- 在重置后的现场提交并跑完一个新构建。

## 实测结果摘要

最新完整测试：

```text
25 passed, 1 warning in 4.64s
```

其中保留原调度器测试 17 个，新增复现/重放测试 8 个，覆盖：

- 两节点和三节点环路径；
- `/tick` 与 `/advance` 结果附复现号；
- 包内初始工作区、成功创建/提交/取消和逐拍顺序；
- 批量推进中插入操作的实际生效顺序；
- 同轮多次报环共享日志、各包截止各自故障拍；
- 下载包重放成功、篡改后 `matched=false` / strict 模式 409；
- 重置后旧号 404、下载 JSON 仍可重放；
- 重放前后现场 `/snapshot` 与 `/events` 不变且现场可继续推进。

真实 HTTP 演练输出：

```text
journal entries=10041, ticks=10000, creates=40, inserts_between_ticks=12
example interleaved operation seq=3020
reproduction number: repro-000001
cycle path: A -> ws-2 -> B -> ws-1 -> A
downloaded package: /tmp/ci-repro-demo/repro-000001.json (62149 bytes)
pre-reset replay: matched=true, failed_build_id=A
replay did not change live snapshot
after reset, old reproduction number: HTTP 404
saved JSON replay after reset: matched=true
live scheduler still advances: fresh=completed
REPRO DEMO OK
```

同时实测 `GET /docs` 返回 HTTP 200，`/openapi.json` 包含
`/reproductions`、`/reproductions/{reproduction_id}/package`、
`/reproductions/{reproduction_id}/replay` 和 `/replay`。

## 目录结构

```text
ci_scheduler/
├── app/
│   ├── main.py             # FastAPI 路由、下载和 strict mismatch
│   ├── replay.py           # 隔离重放器与包格式兼容解析
│   ├── scheduler.py        # 调度核心、环路径、操作日志、复现包
│   └── schemas.py          # 请求/响应模型
├── scripts/
│   ├── demo.py             # 原多级等待 + 取消 HTTP 演练
│   └── repro_demo.py       # 交错推进、下载、重置后重放演练
├── tests/
│   ├── test_scheduler.py
│   └── test_reproduction.py
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 限制 / 余项

1. 当前沙箱没有 `docker` CLI、Compose 插件或可用的 Docker/Podman 守护进程，
   因此未能在此环境真实执行 `docker compose up --build`；已完成 Compose YAML
   解析校验，并用本地 uvicorn 完成真实 HTTP 验证。需在有 Docker 的机器上做
   一次容器启动确认。
2. 复现包和当前轮复现号仍只存内存；进程重启后丢失。下载到客户端的 JSON
   不受影响。
3. 为保证批量推进能被其他请求插入，`/advance` 不在整个批量期间持有调度锁；
   每拍之间是真实的并发插入点。极端 CPU 竞争下，插入请求可能全部排在批量
   前后，这是调度结果而非日志问题；测试用确定性包装器验证顺序记录，HTTP
   演练也实际观察到了拍间插入。
4. `compile` 不真实计时；无认证、无持久化，定位为本地/演练工具。
