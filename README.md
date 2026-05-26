# Subagent Task Dispatcher / 子代理任务调度器

An [AstrBot](https://github.com/Soulter/AstrBot) plugin that provides a `call_subagent` LLM tool for the main agent to dispatch tasks to sub-agents and retrieve results asynchronously.

一个 [AstrBot](https://github.com/Soulter/AstrBot) 插件，为主 agent 提供 `call_subagent` 工具，用于将任务派发给子代理异步执行并获取结果。

---

## Features / 功能

- **Multi-task parallel dispatch** — dispatch multiple sub-agent tasks simultaneously; each runs in the background independently
  **多任务并行派发** — 同时派发多个子代理任务，各任务独立后台运行互不干扰
- **UDP broadcast wakeup** — on task completion, wakes the main agent via UDP loopback + `CronMessageEvent` + `build_main_agent`
  **UDP 广播唤醒** — 任务完成后通过 UDP 自收自发 + CronMessageEvent 唤醒主 agent
- **External UDP wakeup** — external devices can send UDP broadcast packets to wake the LLM
  **外部 UDP 唤醒** — 外部设备可通过 UDP 广播包唤醒 LLM
- **Peer delegation** — sub-agents can delegate subtasks to each other via `call_peer`, with loop detection and depth protection (v1.7.0)
  **子代理间委派** — 子代理之间可通过 `call_peer` 互相委派子任务，内置循环检测和深度保护 (v1.7.0)

---

## How It Works / 工作原理

```
User / 用户
  └─ Main Agent / 主 Agent
       └─ call_subagent(action=dispatch, ...) → returns job_id immediately / 立即返回 job_id
            └─ Background sub-agent task runs independently / 后台子代理任务独立执行
                 └─ On completion / 完成时:
                      ├─ _notify_llm → _wake_up_llm → rebuild main agent via build_main_agent
                      │    └─ Main agent processes result (reply/dispatch again)
                      │       / 主 agent 处理结果（回复用户或继续派发）
                      └─ _send_udp_broadcast → UDP listener / UDP 监听器 → _wake_up_llm (backup / 备用)
```

Key design decisions / 关键设计：

- **Async dispatch / 异步派发**: `dispatch` returns a `job_id` immediately without blocking the main conversation. The main agent can continue replying to the user or dispatch more tasks.
  `dispatch` 立即返回 `job_id`，不阻塞主对话。主 agent 可继续回复用户或派发更多任务。
- **Real session wakeup / 真实会话唤醒**: Uses the original user session to reconstruct the main agent with full toolchain and persona, enabling autonomous decision-making.
  使用原用户会话重建主 agent（含完整工具链和 persona），使其能自主决策。
- **Dual notification path / 双通知路径**: Primary path goes through `_notify_llm` → `_wake_up_llm`. If that fails, UDP broadcast serves as a backup.
  主路径通过 `_notify_llm` → `_wake_up_llm`，失败时 UDP 广播作为备用。

---

## Installation / 安装

Place the plugin directory under AstrBot's `plugins/` directory, or install via the AstrBot WebUI plugin marketplace.

将插件目录放入 AstrBot 的 `plugins/` 目录，或通过 AstrBot WebUI 插件市场安装。

---

## Configuration / 配置

| Key / 配置项 | Type | Default / 默认值 | Description / 说明 |
|-------------|------|-------------------|-------------------|
| `agent_a` | string | `""` | Preferred sub-agent A name. Auto-selects first available if empty. / 首选子代理 A，留空自动选择 |
| `agent_b` | string | `""` | Preferred sub-agent B name. Leave empty to disable. / 首选子代理 B，留空禁用 |
| `agent_c` | string | `""` | Preferred sub-agent C name. Leave empty to disable. / 首选子代理 C，留空禁用 |
| `inherit_all_tools` | bool | `true` | Sub-agents inherit all LLM tools registered via `@filter.llm_tool`. / 子代理继承所有 @filter.llm_tool 注册的工具 |
| `max_steps` | int | `30` | Max tool call rounds per sub-agent task. / 子代理最大工具调用轮数 |
| `tool_call_timeout` | int | `60` | Single tool call timeout in seconds. / 单次工具调用超时（秒） |
| `task_timeout` | int | `120` | Total task timeout in seconds. Task is cancelled on timeout. / 单次任务总超时（秒），超时自动取消 |
| `max_delegation_depth` | int | `3` | Max peer delegation chain depth. 1=A→B only, 3=A→B→C→D. / 子代理间最大委派深度 |
| `udp_enabled` | bool | `true` | Enable UDP broadcast listener. / 启用 UDP 广播监听 |
| `udp_port` | int | `25001` | UDP listen port. / UDP 监听端口 |
| `udp_magic` | string | `astrbot_subagent_wakeup` | Magic phrase; only packets containing this trigger a wakeup. / 唤醒暗号，仅匹配此暗号的包触发唤醒 |
| `udp_self_only` | bool | `true` | Reject UDP packets from non-local addresses. / 仅接受本机 UDP 包 |
| `notify_on_complete` | bool | `true` | Wake the main agent after each sub-agent task completes. / 任务完成后主动唤醒主 agent |

**Agent selection logic** (when `agent_name` is not specified in `dispatch`) / **子代理选择逻辑**（dispatch 未指定 agent_name 时）:
1. Try `agent_a` → `agent_b` → `agent_c` in order, pick the first one that exists / 按顺序尝试，取第一个存在的
2. If none configured, pick the first available sub-agent / 均未配置时取第一个可用子代理

---

## Usage / 使用方法

### Main Agent: `call_subagent` / 主 Agent 工具

Registered as an LLM tool. The main agent can call it with the following actions:
注册为 LLM 工具，主 agent 可调用以下 action：

#### `action=dispatch` — 派发后台任务

```
call_subagent action=dispatch request_text="Check server status" agent_name="tool_executor"
```

Returns immediately with a `job_id` / 立即返回 job_id：

```
✅ 任务已派发给 'tool_executor'，job_id=job_1 (14:30:00)（完成后会自动唤醒我）
请继续回答用户或处理其他事务，任务完成后会自动唤醒你。
届时使用 call_subagent action=fetch job_id=job_1 获取结果。
```

The task runs in background. The main agent is woken automatically on completion if `notify_on_complete` is enabled.
任务在后台执行。`notify_on_complete` 启用时主 agent 会在完成后自动被唤醒。

#### `action=status` — 查看任务状态

```
call_subagent action=status
```

#### `action=fetch` — 获取任务结果

```
call_subagent action=fetch job_id=job_1
```

Result is returned and cleaned from memory / 返回结果并自动清除内存。

#### `action=cancel` — 取消任务

```
call_subagent action=cancel job_id=job_2
```

### Sub-agent: `call_peer` / 子 Agent 间委派

Sub-agents can delegate subtasks to each other via the `call_peer` tool:
子代理可通过 `call_peer` 工具互相委派子任务：

```
call_peer agent_name="visualizer" request_text="Generate a bar chart from the following data: ..."
```

**Safety mechanisms / 安全机制**:

| Mechanism / 机制 | Description / 说明 |
|-----------------|-------------------|
| Depth limit / 深度限制 | `max_delegation_depth` config (default 3), prevents infinite chaining / 防止无限链式委派 |
| Loop detection / 循环检测 | Detects duplicate agent names in the delegation chain / 检测委派链中的重复代理名 |
| Timeout propagation / 超时传导 | Outer `task_timeout` applies to the entire chain / 外层 task_timeout 兜底整个链 |
| Async isolation / 异步隔离 | Peer delegation runs in background, does not block main conversation / 后台执行，不阻塞主对话 |

### Peer Delegation Flow / 委派流程

```
Main Agent / 主 Agent
  └─ call_subagent(agent="A", request="Analyze system status") → A starts / 开始
       ├─ Step 1...
       ├─ Needs chart → call_peer(agent="B", request="Generate CPU chart") → B executes / 执行
       │    └─ B done → returns chart result to A / 返回图表给 A
       ├─ Needs fix → call_peer(agent="C", request="Fix nginx config") → C executes / 执行
       │    └─ C done → returns fix result to A / 返回修复结果给 A
       └─ A integrates results → returns to main agent (via fetch) / 整合返回
```

---

## UDP Wakeup / UDP 唤醒

### Packet Format / 包格式

**JSON (recommended / 推荐)**:
```json
{
  "magic": "astrbot_subagent_wakeup",
  "event": "task_done",
  "job_id": "job_3",
  "status": "completed",
  "summary": "Server check completed, all OK",
  "error": ""
}
```

**Plain text / 纯文本** (backward compatible / 向后兼容):
```
astrbot_subagent_wakeup Server maintenance completed
```

### Event Types / 事件类型

| Type / 类型 | Description / 说明 |
|------------|-------------------|
| `task_done` | Sub-agent task completed / 子代理任务完成（status: completed/timeout/error/cancelled） |
| `wakeup` | General wakeup / 普通唤醒 |
| `alert` | Alert notification / 告警通知 |

### Send Example / 发送示例 (Linux)

```bash
echo '{"magic":"astrbot_subagent_wakeup","event":"task_done","job_id":"job_3","status":"completed","summary":"Task done"}' | nc -u -b -w1 255.255.255.255 25001
```

---

## Plugin API Reference / API 参考

### `call_subagent` (LLM Tool / LLM 工具)

| Parameter / 参数 | Required / 必填 | Description / 说明 |
|-----------------|----------------|-------------------|
| `action` | Yes / 是 | `dispatch` / `status` / `fetch` / `cancel` / `stop` |
| `request_text` | For `dispatch` / dispatch 时必填 | Task description for the sub-agent / 子代理任务描述 |
| `agent_name` | No / 否 | Target sub-agent name; auto-select if empty / 目标子代理名，留空自动选择 |
| `max_steps` | No / 否 | Max tool call steps for this dispatch / 本次派发的最大工具调用步数 |
| `job_id` | For `fetch`/`cancel` | The job ID to retrieve or cancel / 要获取或取消的任务 ID |
| `brief` | No / 否 | Short label (max 10 chars) for quick identification / 简短标签（最多10字），用于快速识别任务 |

### `call_peer` (Sub-agent Tool / 子 Agent 工具)

| Parameter / 参数 | Required / 必填 | Description / 说明 |
|-----------------|----------------|-------------------|
| `agent_name` | Yes / 是 | Target sub-agent name / 目标子代理名 |
| `request_text` | Yes / 是 | Task description to delegate / 委派的任务描述 |
| `max_steps` | No / 否 | Max steps for the target agent (default: plugin config) / 目标子代理最大步数（默认使用插件配置） |

---

## Notify on Complete / 完成通知

When `notify_on_complete = true` (default / 默认):
- On sub-agent task completion, the main agent is woken in background
  子代理任务完成后在后台唤醒主 agent
- The reconstructed main agent has full toolchain and persona
  重建的主 agent 携带完整工具链和 persona
- The agent autonomously decides whether to reply to the user or dispatch more tasks
  agent 自主决策：回复用户或继续派发

When `notify_on_complete = false`:
- Task results are stored in memory and logged
  任务结果存入内存并记录日志
- The main agent can check results on next user interaction via `call_subagent action=status`
  主 agent 可在下次与用户交互时通过 `call_subagent action=status` 查看结果

---

## Version History / 版本历史

- **v1.7.0** — Peer delegation (`call_peer`), loop detection, depth protection / 子代理间委派、循环检测、深度保护
- **v1.6.0** — Real session wakeup via `CronMessageEvent` + `build_main_agent` / 真实会话唤醒
- **v1.4.0** — UDP broadcast wakeup / UDP 广播唤醒
- **v1.0.0** — Basic dispatch with polling / 基础派发 + 轮询
