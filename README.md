<p align="center">
  <a href="#中文文档">🇨🇳 中文</a> | <a href="#english-docs">🇬🇧 English</a>
</p>

---

<a id="中文文档"></a>

# 子代理任务调度器

一个 [AstrBot](https://github.com/Soulter/AstrBot) 插件，为主 agent 提供 `call_subagent` 工具，用于将任务派发给子代理异步执行并获取结果。主要实现：agent派发任务后，任务完成、超时、失败都会主动唤醒agent，agent在后台根据情况继续转派或通知用户。

## 功能

- **多任务并行派发** — 同时派发多个子代理任务，各任务独立后台运行互不干扰
- **UDP 广播唤醒** — 任务完成后通过 UDP 自收自发 + CronMessageEvent 唤醒主 agent
- **外部 UDP 唤醒** — 外部设备可通过 UDP 广播包唤醒 LLM
- **子代理间委派** — 子代理之间可通过 `call_peer` 互相委派子任务，内置循环检测和深度保护

## 工作原理

```
用户
  └─ 主 Agent
       └─ call_subagent(action=dispatch, ...) → 立即返回 job_id
            └─ 子代理后台独立执行
                 └─ 完成时:
                      ├─ _notify_llm → _wake_up_llm → build_main_agent 重建主 agent
                      │    └─ 主 agent 处理结果（回复用户或继续派发）
                      └─ UDP 广播 → 监听器 → _wake_up_llm（备用路径）
```

关键设计：
- **异步派发**：`dispatch` 立即返回 `job_id`，不阻塞主对话
- **真实会话唤醒**：使用原用户会话重建主 agent（含完整工具链和 persona），支持自主决策
- **双通知路径**：主路径 `_notify_llm` → `_wake_up_llm`，失败时 UDP 广播作为备用

## 安装

将插件目录放入 AstrBot 的 `plugins/` 目录，或通过 AstrBot WebUI 插件市场安装。

## 配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `agent_a` | string | `""` | 首选子代理 A，留空自动选择 |
| `agent_b` | string | `""` | 首选子代理 B，留空禁用 |
| `agent_c` | string | `""` | 首选子代理 C，留空禁用 |
| `inherit_all_tools` | bool | `true` | 子代理是否继承所有 LLM 工具 |
| `max_steps` | int | `30` | 子代理最大工具调用轮数 |
| `tool_call_timeout` | int | `60` | 单次工具调用超时（秒） |
| `task_timeout` | int | `120` | 单次任务总超时（秒），超时自动取消 |
| `max_delegation_depth` | int | `3` | 子代理间最大委派深度 |
| `udp_enabled` | bool | `true` | 启用 UDP 广播监听 |
| `udp_port` | int | `25001` | UDP 监听端口 |
| `udp_magic` | string | `astrbot_subagent_wakeup` | 唤醒暗号，仅匹配此暗号的包触发唤醒 |
| `udp_self_only` | bool | `true` | 仅接受本机 UDP 包 |
| `notify_on_complete` | bool | `true` | 任务完成后是否唤醒主 agent |

**子代理选择逻辑**（dispatch 未指定 agent_name 时）：
1. 按 `agent_a` → `agent_b` → `agent_c` 顺序，取第一个存在的
2. 均未配置时取第一个可用子代理

## 使用方法

### 主 Agent: `call_subagent`

注册为 LLM 工具，主 agent 可调用以下 action：

#### `action=dispatch` — 派发后台任务

```
call_subagent action=dispatch request_text="检查服务器状态" agent_name="tool_executor"
```

立即返回 job_id：
```
✅ 任务已派发给 'tool_executor'，job_id=job_1 (14:30:00)（完成后会自动唤醒我）
请继续回答用户或处理其他事务，任务完成后会自动唤醒你。
届时使用 call_subagent action=fetch job_id=job_1 获取结果。
```

任务在后台执行，`notify_on_complete` 启用时主 agent 完成后自动被唤醒。

#### `action=status` — 查看任务状态

```
call_subagent action=status
```

#### `action=fetch` — 获取任务结果

```
call_subagent action=fetch job_id=job_1
```

返回结果并自动清除内存。

#### `action=cancel` — 取消任务

```
call_subagent action=cancel job_id=job_2
```

### 子 Agent: `call_peer`

子代理之间可通过 `call_peer` 互相委派子任务：

```
call_peer agent_name="visualizer" request_text="根据以下数据生成柱状图：..."
```

**安全机制**：

| 机制 | 说明 |
|------|------|
| 深度限制 | `max_delegation_depth` 配置（默认3），防止无限链式委派 |
| 循环检测 | 自动检测委派链中的重复代理名 |
| 超时传导 | 外层 `task_timeout` 兜底整个链 |
| 异步隔离 | 后台执行，不阻塞主对话 |

### 委派流程

```
主 Agent
  └─ call_subagent(agent="A", request="分析系统状态") → A 开始
       ├─ 步骤 1...
       ├─ 需要图表 → call_peer(agent="B", request="生成 CPU 图表") → B 执行
       │    └─ B 完成 → 返回图表给 A
       ├─ 需要修复 → call_peer(agent="C", request="修复 nginx 配置") → C 执行
       │    └─ C 完成 → 返回修复结果给 A
       └─ A 整合结果 → 返回给主 agent（通过 fetch）
```

## UDP 唤醒

### 包格式

**JSON（推荐）**：
```json
{
  "magic": "astrbot_subagent_wakeup",
  "event": "task_done",
  "job_id": "job_3",
  "status": "completed",
  "summary": "服务器巡检完成",
  "error": ""
}
```

**纯文本**（向后兼容）：
```
astrbot_subagent_wakeup 服务器维护完成
```

### 事件类型

| 类型 | 说明 |
|------|------|
| `task_done` | 子代理任务完成（status: completed/timeout/error/cancelled） |
| `wakeup` | 普通唤醒 |
| `alert` | 告警通知 |

### 发送示例

```bash
echo '{"magic":"astrbot_subagent_wakeup","event":"task_done","job_id":"job_3","status":"completed","summary":"巡检完成"}' | nc -u -b -w1 255.255.255.255 25001
```

## 完成通知

`notify_on_complete = true`（默认）：
- 子代理任务完成后在后台唤醒主 agent
- 重建的主 agent 携带完整工具链和 persona
- agent 自主决策：回复用户或继续派发

`notify_on_complete = false`：
- 任务结果存入内存并记录日志
- 主 agent 可在下次交互时通过 `call_subagent action=status` 查看

## API 参考

### `call_subagent`

| 参数 | 必填 | 说明 |
|------|------|------|
| `action` | 是 | `dispatch`/`status`/`fetch`/`cancel`/`stop` |
| `request_text` | dispatch 时 | 子代理任务描述 |
| `agent_name` | 否 | 目标子代理名，留空自动选择 |
| `max_steps` | 否 | 本次派发最大工具调用步数 |
| `job_id` | fetch/cancel 时 | 任务 ID |
| `brief` | 否 | 简短标签（最多10字），用于快速识别 |

### `call_peer`

| 参数 | 必填 | 说明 |
|------|------|------|
| `agent_name` | 是 | 目标子代理名 |
| `request_text` | 是 | 委派的任务描述 |
| `max_steps` | 否 | 目标子代理最大步数（默认插件配置值） |

## 版本历史

- **v1.7.0** — 子代理间委派（`call_peer`）、循环检测、深度保护
- **v1.6.0** — 真实会话唤醒（CronMessageEvent + build_main_agent）
- **v1.4.0** — UDP 广播唤醒
- **v1.0.0** — 基础派发 + 轮询

---

<a id="english-docs"></a>

# Subagent Task Dispatcher

An [AstrBot](https://github.com/Soulter/AstrBot) plugin that provides a `call_subagent` LLM tool for the main agent to dispatch tasks to sub-agents and retrieve results asynchronously.

## Features

- **Multi-task parallel dispatch** — dispatch multiple sub-agent tasks simultaneously; each runs in the background independently
- **UDP broadcast wakeup** — on task completion, wakes the main agent via UDP loopback + `CronMessageEvent` + `build_main_agent`
- **External UDP wakeup** — external devices can send UDP broadcast packets to wake the LLM
- **Peer delegation** — sub-agents can delegate subtasks to each other via `call_peer`, with loop detection and depth protection (v1.7.0)

## Quick Start

Place the plugin directory under AstrBot's `plugins/` directory.

### Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `agent_a` | string | `""` | Preferred sub-agent A name. Auto-selects first available if empty. |
| `agent_b` | string | `""` | Preferred sub-agent B name. Leave empty to disable. |
| `agent_c` | string | `""` | Preferred sub-agent C name. Leave empty to disable. |
| `inherit_all_tools` | bool | `true` | Sub-agents inherit all LLM tools registered via `@filter.llm_tool`. |
| `max_steps` | int | `30` | Max tool call rounds per sub-agent task. |
| `tool_call_timeout` | int | `60` | Single tool call timeout (seconds). |
| `task_timeout` | int | `120` | Total task timeout (seconds). Task is cancelled on timeout. |
| `max_delegation_depth` | int | `3` | Max peer delegation chain depth. 1 = A→B only, 3 = A→B→C→D. |
| `udp_enabled` | bool | `true` | Enable UDP broadcast listener. |
| `udp_port` | int | `25001` | UDP listen port. |
| `udp_magic` | string | `astrbot_subagent_wakeup` | Magic phrase; only matching packets trigger a wakeup. |
| `udp_self_only` | bool | `true` | Reject UDP packets from non-local addresses. |
| `notify_on_complete` | bool | `true` | Wake the main agent after each sub-agent task completes. |

### Usage

**Dispatch a task**:
```
call_subagent action=dispatch request_text="Check server status" agent_name="tool_executor"
```

**Check status**: `call_subagent action=status`

**Fetch result**: `call_subagent action=fetch job_id=job_1`

**Cancel task**: `call_subagent action=cancel job_id=job_2`

**Peer delegation** (sub-agent):
```
call_peer agent_name="visualizer" request_text="Generate a bar chart from the following data: ..."
```

## Version History

- **v1.7.0** — Peer delegation (`call_peer`), loop detection, depth protection
- **v1.6.0** — Real session wakeup via `CronMessageEvent` + `build_main_agent`
- **v1.4.0** — UDP broadcast wakeup
- **v1.0.0** — Basic dispatch with polling
