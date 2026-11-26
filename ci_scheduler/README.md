# CI Scheduler Drill — FastAPI 单执行器 + 优先级继承

共享工作区被后台构建占着时的调度演练。每个工作区是互斥资源；构建由
`acquire` / `release` / `compile` 步骤组成。系统只有**一个执行器**，每次
`tick` 让**有效优先级最高的就绪构建恰好走一步**。

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
     因此可沿 `申请者 -> 工作区 -> 持有者` 链多级传递（F→B→A 两跳已实测）。
4. **释放后重新竞争**：工作区释放时，等待队列里有效优先级最高者获得
   （同优先级按提交顺序 `seq` 破平），其余等待者继续等待。
5. **取消**：只允许取消等待中的构建；取消后从等待队列移除，释放它自身
   持有的全部工作区，并仅对受影响的等待链重算优先级。
6. **等待环检测**：`acquire` 若会使 `持有者 -> 等待者 -> 持有者……` 链回到
   申请者，则申请者立即失败（`status=failed`，带 `cycle` 标记和原因），
   并归还它持有的工作区，等待者随之被唤醒重新竞争。
7. **结束归还**：构建完成（最后一步执行完）时释放它占用的全部工作区。
8. **增量重算**：优先级只沿受影响的等待链向上重算，链上某节点值不变即
   提前终止；测试用“从零固定点全量重算”在随机交织下校验二者始终一致。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workspaces` | 新建工作区 |
| GET | `/workspaces` | 工作区占用者与等待队列 |
| POST | `/builds` | 提交构建（基础优先级 + 步骤列表） |
| GET | `/builds` | 全部构建（含有效优先级、等待对象 `waiting_on/blocked_by`） |
| GET | `/builds/{id}` | 单个构建详情 |
| POST | `/builds/{id}/cancel` | 取消等待中的构建 |
| POST | `/tick` | 单执行器推进一步 |
| POST | `/advance` | 推进多步 |
| GET | `/events?limit=n` | 每步结果与全部状态变更事件 |
| GET | `/snapshot` | 工作区 + 构建完整快照 |
| POST | `/reset` | 重置工作区集合与全部状态 |

启动后交互式文档：<http://localhost:8000/docs>（Swagger UI，可直接试跑）。

## 用 Docker Compose 启动

```bash
cd ci_scheduler
docker compose up --build
# 打开 http://localhost:8000/docs
```

## 本地（无 Docker 时）开发与测试

```bash
# 依赖装到独立目录，不污染系统 Python
python3 -m pip install --target=../.pylibs -r requirements.txt pytest httpx
PYTHONPATH=../.pylibs python3 -m pytest tests/ -q
PYTHONPATH=../.pylibs python3 -m uvicorn app.main:app --port 8000
PYTHONPATH=../.pylibs python3 scripts/demo.py            # 端到端演练
```

## 实测结果

见 `TEST_REPORT.md`。测试套件 17 个用例全部通过；`scripts/demo.py` 通过
真实 HTTP 调用验证了两跳优先级继承、释放重新竞争与取消。

## 目录结构

```
ci_scheduler/
├── app/
│   ├── main.py          # FastAPI 路由
│   ├── scheduler.py     # 调度器核心（继承、环检测、增量重算）
│   └── schemas.py       # 请求/响应模型
├── tests/test_scheduler.py
├── scripts/demo.py      # HTTP 端到端演练（多级等待 + 取消）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
