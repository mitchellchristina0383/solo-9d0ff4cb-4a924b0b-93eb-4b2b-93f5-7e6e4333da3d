# 实测报告

环境：Python 3.11.2、FastAPI 0.165.0、uvicorn 0.54.0、pytest 9.2.0（依赖安装
在仓库外的独立目录 `.pylibs/`，通过 `PYTHONPATH` 引用）。

## 一、实现的功能

### 调度核心（`app/scheduler.py`）

1. **单执行器推进**：`tick()` 每次从所有 `ready` 构建中选有效优先级最高者
   执行一个步骤；同优先级按提交顺序破平；无就绪构建返回 `noop`。
2. **三类步骤**：
   - `acquire`：空闲即占用；被占则进入 `waiting` 并加入该工作区等待队列。
   - `release`：释放工作区，触发等待者重新竞争。
   - `compile`：模拟编译，记录 `duration_ms`，不实际阻塞。
3. **优先级继承**：
   - 有效优先级 `ep(x) = max(base(x), 所有等待 x 所持工作区的构建的 ep)`。
   - 沿等待森林多级传递（已验证两跳链 F→B→A）。
   - 持有者只升不降：收到的优先级低于自身基础值时保持基础值。
4. **释放重新竞争**：等待队列中有效优先级最高者获得工作区（同值按
   `seq` 破平），转为就绪；其余等待者保持等待。
5. **取消**：仅等待中的构建可取消；出队、释放自身持有工作区、对受影响
   等待链做增量重算。取消运行中/已结束构建返回 400。
6. **等待环检测**：`acquire` 前沿 `持有者 -> 等待者` 链做可达性检查，
   闭环则申请者 `failed`（事件带 `cycle: true` 与原因），归还其工作区，
   被阻塞的等待者立即被唤醒。已覆盖两节点环与三节点环。
7. **结束归还**：构建执行完最后一步自动 `completed` 并释放全部工作区，
   不泄漏资源。
8. **增量重算**：仅遍历受影响等待链，节点有效值未变即提前终止；
   `_full_recompute()` 提供从零固定点全量重算用于对照测试。
9. 线程安全：所有状态操作在同一把 `RLock` 下完成（FastAPI 线程池下安全）。

### HTTP 接口（`app/main.py`）

- 工作区：`POST/GET /workspaces`
- 构建：`POST /builds`、`GET /builds`、`GET /builds/{id}`、
  `POST /builds/{id}/cancel`
- 调度：`POST /tick`、`POST /advance`
- 可观测：`GET /events`（每步结果 + 状态变更，可 `limit`）、
  `GET /snapshot`（含 `effective_priority`、`waiting_on`、`blocked_by`）
- 系统：`POST /reset`
- 输入校验由 Pydantic 负责（未知工作区、重复 ID 等领域错误返回 400，
  请求体结构错误返回 422）。
- Swagger UI：`GET /docs`，实测 HTTP 200。

## 二、实测结果

### 1. 单元/接口测试：17/17 通过

```
$ PYTHONPATH=.pylibs python3 -m pytest tests/ -v
test_highest_effective_priority_ready_build_runs_first            PASSED
test_single_executor_advances_one_step_per_tick                  PASSED
test_tie_breaks_by_submission_order                               PASSED
test_basic_priority_inheritance_on_blocked_acquire                PASSED
test_holder_takes_max_of_own_and_received_priority                PASSED
test_multi_level_wait_chain_inherits_transitively                      PASSED
test_priority_drops_when_waiting_chain_disappears                 PASSED
test_cycle_request_makes_requester_fail                           PASSED
test_three_node_cycle_is_detected                                 PASSED
test_cancel_waiting_build_recompetes_and_releases_pressure        PASSED
test_cancel_waiting_holder_unwinds_inherited_priority             PASSED
test_release_recompetes_waiters_by_effective_priority             PASSED
test_completed_build_returns_all_workspaces                       PASSED
test_events_and_snapshot_expose_effective_priority_and_waits      PASSED
test_incremental_recompute_matches_full_recompute                 PASSED
test_incoming_below_base_priority_does_not_lower_effective        PASSED
test_validation_errors                                            PASSED
======================== 17 passed in 4.19s =========================
```

重点覆盖：

- **多级等待**：F(7) 等 ws-2（B 持有），B(4) 等 ws-1（A 持有，base=1）。
  快照实测 `A.effective = B.effective = 7`；插入 base=6 的就绪构建 N，
  执行器仍选 A（ep 7 > 6），证明 A 是靠继承才抢到执行器。A 释放 ws-1 后
  B 被唤醒，B 释放 ws-2 后 F 被唤醒，全部正常完成、工作区全部回收。
- **取消**：H 持有 ws-1，lo(4)、hi(9) 依次排队；`H.effective=9`。
  取消 hi 后实测 `H.effective` 立刻回落到 4，等待队列只剩 lo。
  另测“自身也持有工作区的等待者被取消”：ws-2 转交给 F，上游 A 的继承
  优先级从 7 回落到基础值 1。
- **增量重算正确性**：固定随机种子的 12 个等待构建 + 3 个持有者、80 个
  tick 并随机取消，每步把增量结果与全量固定点重算对比，全程一致。
- **环**：两节点交叉持锁环（申请者 failed 并释放，对方被唤醒跑完）；
  三节点环 A→B→C→A 同样被识别。

### 2. 真实 HTTP 端到端演练（`scripts/demo.py`）

对本地 uvicorn 实例发真实请求，关键实测输出：

```
=== two-hop wait chain F -> B -> A ===
ws-1: held_by=A, waiters=[B]
ws-2: held_by=B, waiters=[F]
A: base=1 effective=7   B: base=4 effective=7   F: base=7 effective=7
[tick] executor picked A (ep 7) over ready N (ep 6)
... 链排空后 A/B/F/N 全部 completed，工作区 held_by=null、waiters=[]
before cancel: H.effective = 9
after cancel(hi): H.effective = 4 (dropped back to lo's 4)
cancel of running build rejected: HTTP 400
DEMO OK: multi-level wait, inheritance, release re-competition and
cancellation all verified over HTTP.
```

`/advance` 批量推进实测：2 步构建用 `steps=5` 调用，返回 2 个 step 事件 +
1 个提前停止的 `noop`。

### 3. 部署文件

- `docker-compose.yml` 通过 YAML 解析与结构校验（服务 `ci-scheduler`、
  端口 8000、healthcheck 齐备）；`Dockerfile` 基于 `python:3.11-slim`，
  `requirements.txt` 锁定版本。
- **本沙箱未安装 Docker 守护进程**，无法实际执行
  `docker compose up --build`，该路径仅做了静态校验（见未完成项）。

## 三、未完成项 / 限制

1. **Docker 未实跑**：当前环境无 `docker` 命令，Compose 文件只做了静态
   校验；需在有 Docker 的机器上执行 `docker compose up --build` 后访问
   `/docs` 做最终确认。应用本身与容器内启动命令
   （`uvicorn app.main:app --host 0.0.0.0 --port 8000`）已在本地直接验证。
2. **状态仅在内存**：进程重启后构建/工作区/事件全部丢失，未做持久化。
3. **compile 不真实计时**：只记录 `duration_ms` 作为模拟信息，不会占用
   墙钟时间，也没有超时/抢占机制。
4. **单进程单实例**：调度器是模块级单例，无多副本水平扩展设计；
   取消仅支持等待中的构建（运行中构建不可取消，符合需求约定）。
5. 无认证/鉴权与速率限制，定位为本地演练工具。
