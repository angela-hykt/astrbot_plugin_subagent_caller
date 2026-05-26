"""
子代理任务调度器插件 - v1.7.0

为主 agent 提供 `call_subagent` 工具，用于将任务派发给子代理异步执行。
支持多任务并行派发、UDP 广播唤醒和子代理间通信委派：
  - 任务完成后通过 UDP 自收自发 + CronMessageEvent 唤醒主 agent
  - 外部设备可通过 UDP 广播包唤醒 LLM
  - 子代理之间可通过 call_peer 工具互相委派子任务

配置项（WebUI 插件配置页可设置）：
  - inherit_all_tools: 是否继承所有 LLM 工具，默认 true
  - max_steps: 子代理最大工具调用轮数，默认 30
  - tool_call_timeout: 单次工具调用超时（秒），默认 60
  - max_delegation_depth: 子代理间最大委派深度，默认 3
  - udp_enabled: 是否启用 UDP 广播唤醒
  - udp_port: UDP 监听端口，默认 25001
  - udp_magic: UDP 唤醒暗号，默认 "astrbot_subagent_wakeup"
  - notify_on_complete: 任务完成后是否唤醒主 agent

UDP 广播包格式（两种均支持）：
  纯文本: <暗号> <附加信息>
  JSON:   {"magic":"<暗号>","event":"task_done","job_id":"job_xxx","status":"completed","summary":"...","error":"..."}

事件类型:
  task_done  - 子代理任务完成（status: completed/timeout/error/cancelled）
  wakeup     - 普通唤醒
  alert      - 告警通知

唤醒机制：
  通过 CronMessageEvent + build_main_agent 重建主 agent，
  携带完整工具链和 persona 处理后台任务通知。

子代理间委派（v1.7.0）：
  子代理之间可通过 call_peer 工具互相委派子任务，
  支持多级链式委派（A→B→C），内置循环检测和深度保护。
"""

from __future__ import annotations

import asyncio
import json
import re
import socket
import time
from typing import TYPE_CHECKING, Any

from astrbot.api import logger
from astrbot.api.event import filter
from astrbot.api.star import Star, register
from astrbot.core.agent.hooks import BaseAgentRunHooks
from astrbot.core.agent.message import Message
from astrbot.core.agent.runners.tool_loop_agent_runner import ToolLoopAgentRunner
from astrbot.core.agent.tool import FunctionTool, ToolSet
from astrbot.core.astr_agent_context import AstrAgentContext, AgentContextWrapper
from astrbot.core.astr_agent_tool_exec import FunctionToolExecutor
from astrbot.core.provider.entities import ProviderRequest

from astrbot.core.cron.events import CronMessageEvent
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.astr_main_agent import (
    _get_session_conv,
    build_main_agent,
    MainAgentBuildConfig,
)

if TYPE_CHECKING:
    from astrbot.api.event import AstrMessageEvent
    from astrbot.api.star import Context


_BLOCKED_PATTERNS = re.compile(
    r"(?i)\b(rm\s+(-rf?|--recursive|--no-preserve-root)\s+(/|/\*)|"
    r"mkfs\.\w+|dd\s+if=/dev/zero|"
    r":\(\)\s*\{|"
    r">?\s*/dev/(sda|sdb|sdc|nvme|mmc)|"
    r"chmod\s+777\s+/|"
    r"chown\s+|"
    r"passwd\s+|"
    r"(halt|poweroff|reboot|shutdown|init\s+0|systemctl\s+(poweroff|halt|reboot))|"
    r"wget\s+.*\||curl\s+.*\||"
    r"\b(python|perl|ruby|bash)\s+.*(\bhttp|/tmp|/dev/)|"
    r";\s*(rm|mkfs|dd|chmod|chown|wget|curl|halt|reboot|poweroff|shutdown|init)|"
    r"\|\s*(sh|bash|dash|zsh)\b|"
    r"(sudo|su\s+)\s+(rm|mkfs|dd|chmod|chown|halt|poweroff|reboot|shutdown|init|passwd)|"
    r"`[^`]*`|"
    r"\$\([^)]+\))",
    re.DOTALL,
)


async def _shell_executor(event, command: str, timeout: int = 300, background: bool = False) -> str:
    """在服务器上执行 shell 命令并返回输出。仅由插件调度的子代理使用。"""
    logger.info(f"[SubagentCaller] shell执行: {command[:200]}")

    if _BLOCKED_PATTERNS.search(command):
        return (
            f"❌ 命令被安全策略拦截（匹配到危险模式）: {command[:100]}"
        )

    try:
        if background:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            return f"已在后台启动命令: {command}"

        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            try:
                process.kill()
            except Exception:
                pass
            return f"命令执行超时(>{timeout}s): {command}"

        parts = []
        if stdout:
            parts.append(stdout.decode("utf-8", errors="replace"))
        if stderr:
            parts.append(f"[STDERR]\n{stderr.decode('utf-8', errors='replace')}")
        result = "\n".join(parts)
        return result if result.strip() else "(无输出)"
    except Exception as e:
        return f"执行失败: {e}"


_SHELL_TOOL = FunctionTool(
    name="subagent_shell_exec",
    description=(
        "在服务器上执行shell命令。支持普通执行和后台运行。"
        "注意：此工具仅在通过call_subagent调度的子代理中可用。"
    ),
    parameters={
        "type": "object",
        "properties": {
            "command": {
                "type": "string",
                "description": "要执行的shell命令。支持管道、重定向等标准shell语法。"
            },
            "timeout": {
                "type": "integer",
                "description": "超时时间（秒），默认300。后台运行时不生效。",
                "default": 300
            },
            "background": {
                "type": "boolean",
                "description": "是否在后台运行。true=立即返回不等待结果，false=等待执行完毕。默认false。",
                "default": False
            }
        },
        "required": ["command"]
    },
    handler=_shell_executor,
)


@register(
    "dangerous_subagent_caller",
    "angela-hykt",
    "为主 agent 提供 call_subagent 工具，用于将任务派发给子代理异步执行。子代理继承 shell 执行和子代理间委派能力。",
    "1.7.0",
)
class SubagentCaller(Star):

    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        cfg = config or {}
        self.agent_a = str(cfg.get("agent_a", "")).strip()
        self.agent_b = str(cfg.get("agent_b", "")).strip()
        self.agent_c = str(cfg.get("agent_c", "")).strip()
        self.inherit_all_tools = bool(cfg.get("inherit_all_tools", True))
        self.max_steps = max(1, int(cfg.get("max_steps", 30)))
        self.tool_call_timeout = max(5, int(cfg.get("tool_call_timeout", 60)))
        self.task_timeout = max(10, int(cfg.get("task_timeout", 120)))
        self.max_delegation_depth = max(0, int(cfg.get("max_delegation_depth", 3)))
        self.auto_notify = bool(cfg.get("auto_notify", True))

        # UDP广播唤醒配置
        self.udp_enabled = bool(cfg.get("udp_enabled", True))
        self.udp_port = max(1024, min(65535, int(cfg.get("udp_port", 25001))))
        self.udp_magic = str(cfg.get("udp_magic", "astrbot_subagent_wakeup")).strip()
        self.udp_self_only = bool(cfg.get("udp_self_only", True))
        if not self.udp_self_only:
            logger.warning(
                "[SubagentCaller] ⚠️ udp_self_only=False: UDP监听接受外部设备广播包。"
                "任务摘要（最长200字符）将以明文发送到局域网广播地址。"
                "不要在公共/共享网络（如咖啡厅WiFi、宿舍网络）开启此选项。"
            )
        self.notify_on_complete = bool(cfg.get("notify_on_complete", True))

        # 多任务轮询调度 - 后台任务管理器
        self._tasks: dict[str, asyncio.Task] = {}
        self._results: dict[str, str] = {}
        self._running_jobs: dict[str, float] = {}
        self._completion_times: dict[str, float] = {}
        self._task_briefs: dict[str, str] = {}
        self._task_lock = asyncio.Lock()
        self._task_counter = 0
        # 最多保留 100 条未取走的结果，防止内存泄漏
        self._max_results = 100

        # UDP 监听 & 唤醒相关
        self._udp_task: asyncio.Task | None = None
        self._last_session_id: str | None = None
        self._last_provider_id: str | None = None
        self._notified_jobs: set[str] = set()
        self._max_notified = 500
        self._wake_lock = asyncio.Lock()

        # 启动 UDP 监听
        if self.udp_enabled:
            self._udp_task = asyncio.create_task(self._udp_listener_loop())

            def _check_udp_start(loop_task):
                if loop_task.done() and not loop_task.cancelled():
                    try:
                        loop_task.result()
                    except Exception as e:
                        logger.error(f"[SubagentCaller] UDP监听异常退出: {e}")
                        self._udp_task = None

            self._udp_task.add_done_callback(_check_udp_start)
            logger.info(
                f"[SubagentCaller] UDP广播唤醒已启动 | "
                f"端口={self.udp_port} 暗号='{self.udp_magic}'"
            )

        # 子代理间通信工具 - call_peer（注入到子代理 toolset 中）
        self._peer_tool = FunctionTool(
            name="call_peer",
            description=(
                "将子任务委派给另一个子代理执行，并等待其返回结果。"
                "用于子代理之间互相沟通协作。"
                "注意：此工具仅在子代理上下文中可用，主代理请使用 call_subagent。"
            ),
            parameters={
                "type": "object",
                "properties": {
                    "agent_name": {
                        "type": "string",
                        "description": "目标子代理的名称。使用 list_available_agents 或查看配置了解可用子代理。"
                    },
                    "request_text": {
                        "type": "string",
                        "description": "要委派给目标子代理执行的任务描述，越详细越好。"
                    },
                    "max_steps": {
                        "type": "integer",
                        "description": "可选，目标子代理的最大工具调用步数，默认使用插件配置值。",
                        "default": -1
                    }
                },
                "required": ["agent_name", "request_text"]
            },
            handler=self._on_call_peer,
        )

        logger.info(
            f"[SubagentCaller] 已加载 v1.7.0 | "
            f"agent_a={self.agent_a or '(未配置)'} agent_b={self.agent_b or '(未配置)'} agent_c={self.agent_c or '(未配置)'} "
            f" | "
            f"inherit_all_tools={self.inherit_all_tools} | "
            f"max_steps={self.max_steps} tool_timeout={self.tool_call_timeout}s task_timeout={self.task_timeout}s max_depth={self.max_delegation_depth} | "
            f"auto_notify={self.auto_notify} | "
            f"udp_enabled={self.udp_enabled} udp_self_only={self.udp_self_only} notify_on_complete={self.notify_on_complete} | "
            f"多任务轮询调度已启用"
        )

    async def terminate(self):
        """插件卸载时清理 UDP 监听任务。"""
        if self._udp_task:
            self._udp_task.cancel()
            try:
                await self._udp_task
            except (asyncio.CancelledError, Exception):
                pass
            self._udp_task = None
            logger.info("[SubagentCaller] UDP监听已停止")

    # ─────────────────────────────────────────────
    # UDP广播监听器
    # ─────────────────────────────────────────────

    async def _udp_listener_loop(self):
        """UDP 广播监听循环。收到匹配暗号的包后唤醒 LLM。"""
        loop = asyncio.get_running_loop()
        transport: asyncio.DatagramTransport | None = None
        sock = None

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.setblocking(False)
            sock.bind(("0.0.0.0", self.udp_port))

            class _UdpProtocol(asyncio.DatagramProtocol):
                def __init__(self, plugin):
                    self.plugin = plugin
                    self.transport = None

                def connection_made(self, transport):
                    self.transport = transport

                def datagram_received(self, data, addr):
                    # 在事件循环中调度处理，避免阻塞协议回调
                    asyncio.ensure_future(
                        self.plugin._on_udp_packet(data, addr)
                    )

                def error_received(self, exc):
                    logger.error(f"[SubagentCaller] UDP协议错误: {exc}")

                def connection_lost(self, exc):
                    if exc:
                        logger.warning(f"[SubagentCaller] UDP连接断开: {exc}")

            # 创建 UDP 协议实例
            protocol = _UdpProtocol(self)
            transport, _ = await loop.create_datagram_endpoint(
                lambda: protocol,
                sock=sock,
            )

            logger.info(
                f"[SubagentCaller] UDP监听已就绪 | "
                f"0.0.0.0:{self.udp_port} 暗号='{self.udp_magic}'"
            )

            # 保持运行直到被取消
            await asyncio.get_running_loop().create_future()

        except asyncio.CancelledError:
            logger.info("[SubagentCaller] UDP监听任务已取消")
        except OSError as e:
            logger.error(
                f"[SubagentCaller] UDP监听启动失败(端口{self.udp_port}): {e}\n"
                f"如需使用请更换端口或关闭UDP唤醒功能"
            )
        except Exception as e:
            logger.error(f"[SubagentCaller] UDP监听异常: {e}", exc_info=True)
        finally:
            if transport:
                transport.close()
            elif sock:
                sock.close()

    async def _on_udp_packet(self, data: bytes, addr: tuple):
        """处理收到的 UDP 数据包。

        支持两种包格式：
        1. JSON 结构化：
           {"magic":"<暗号>","event":"task_done","job_id":"job_xxx",
            "status":"completed","summary":"done","error":""}
        2. 纯文本（向后兼容）：
           <暗号> <附加信息>
        """
        try:
            raw = data.decode("utf-8", errors="replace").strip()
            if not raw:
                return

            logger.info(f"[SubagentCaller] 收到UDP包来自 {addr[0]}:{addr[1]} -> '{raw[:128]}'")

            # ── IP 校验：仅自收自发模式 ──
            if self.udp_self_only:
                src_ip = addr[0]
                # 只允许 127.0.0.1 或本机内网 IP
                if src_ip not in ("127.0.0.1", "::1", "localhost"):
                    # 获取本机IP列表
                    local_ips = set()
                    try:
                        local_ips.add(socket.gethostbyname(socket.gethostname()))
                    except Exception:
                        pass
                    try:
                        for _info in socket.getaddrinfo(socket.gethostname(), None):
                            ip = _info[4][0]
                            if not ip.startswith("127.") and ":" not in ip:
                                local_ips.add(ip)
                    except Exception:
                        pass
                    if src_ip not in local_ips:
                        logger.info(f"[SubagentCaller] 自收自发模式，忽略外部包来自 {src_ip}")
                        return

            # ── 解析包内容 ──
            event_type = "wakeup"
            job_id = ""
            task_status = ""
            summary = ""
            error_msg = ""

            # 尝试 JSON 解析
            parsed_json = False
            try:
                pkt = json.loads(raw)
                if isinstance(pkt, dict):
                    magic = str(pkt.get("magic", "")).strip()
                    if magic and self.udp_magic and magic != self.udp_magic:
                        return  # 暗号不匹配
                    event_type = str(pkt.get("event", "wakeup")).strip()
                    job_id = str(pkt.get("job_id", "")).strip()
                    task_status = str(pkt.get("status", "")).strip()
                    summary = str(pkt.get("summary", "")).strip()
                    error_msg = str(pkt.get("error", "")).strip()
                    parsed_json = True
            except (json.JSONDecodeError, ValueError, TypeError):
                pass

            # 纯文本兜底
            if not parsed_json:
                if self.udp_magic and not raw.strip().startswith(self.udp_magic):
                    return
                idx = raw.find(self.udp_magic) if self.udp_magic else -1
                if idx >= 0:
                    summary = raw[idx + len(self.udp_magic or ""):].strip()
                event_type = "wakeup"

            logger.info(
                f"[SubagentCaller] UDP唤醒触发, 来自 {addr[0]} | "
                f"event={event_type} job_id={job_id or '(无)'} "
                f"status={task_status or '(无)'}"
            )

            # ── 防重复：task_done 事件中，如果 job 已被 _notify_llm 处理则跳过 ──
            skip_wake = False
            if event_type.strip().casefold() == "task_done" and job_id:
                async with self._task_lock:
                    if job_id in self._notified_jobs:
                        skip_wake = True
            if skip_wake:
                logger.info(
                    f"[SubagentCaller] UDP task_done 已由 _notify_llm 处理，跳过唤醒: "
                    f"job_id={job_id}"
                )
            else:
                wake_msg = self._build_wakeup_message(
                    event_type, job_id, task_status, summary, error_msg, addr[0]
                )
                await self._wake_up_llm(wake_msg, job_id=job_id)

        except Exception as e:
            logger.error(f"[SubagentCaller] 处理UDP包失败: {e}")

    def _build_wakeup_message(
        self,
        event_type: str,
        job_id: str,
        status: str,
        summary: str,
        error: str,
        source_ip: str,
    ) -> str:
        """根据事件类型构建唤醒消息内容。"""
        et = event_type.strip().casefold()

        if et == "task_done":
            status_icon = ""
            status_text = ""
            if status == "completed":
                status_icon = "✅"
                status_text = "已完成"
            elif status == "timeout":
                status_icon = "⚠️"
                status_text = "超时了"
            elif status == "error":
                status_icon = "❌"
                status_text = "出错了"
            elif status == "cancelled":
                status_icon = "🛑"
                status_text = "被取消了"
            else:
                status_icon = "ℹ️"
                status_text = f"状态={status}"

            parts = [f"[系统UDP通知] 来自 {source_ip}"]
            if job_id:
                parts.append(f"job_id={job_id}")
            if status_text:
                parts.append(f"{status_icon}{status_text}")
            if summary:
                safe = summary[:200].replace("<", "＜").replace(">", "＞")
                parts.append(f"，内容：{safe}")
            if error and error not in summary:
                safe_err = error[:200].replace("<", "＜").replace(">", "＞")
                parts.append(f"\n错误信息：{safe_err}")
                parts.append(f"\n请检查。")
            return " ".join(parts)

        elif et == "alert":
            parts = [f"[系统UDP告警] 来自 {source_ip}"]
            if summary:
                safe = summary[:300].replace("<", "＜").replace(">", "＞")
                parts.append(f"：{safe}")
            if job_id:
                parts.append(f" (job_id={job_id})")
            return " ".join(parts)

        else:
            msg = f"[系统UDP唤醒] 收到来自 {source_ip} 的唤醒信号"
            if summary:
                safe = summary[:300].replace("<", "＜").replace(">", "＞")
                msg += f"：{safe}"
            if job_id:
                msg += f" (job_id={job_id})"
            return msg

    # ─────────────────────────────────────────────
    # UDP广播发送
    # ─────────────────────────────────────────────

    async def _send_udp_broadcast(
        self,
        event_type: str,
        job_id: str = "",
        status: str = "",
        summary: str = "",
        error: str = "",
    ) -> None:
        """向局域网发送 UDP 广播包（自收自发），通知监听器有任务完成。"""
        sock = None
        try:
            pkt = {
                "magic": self.udp_magic,
                "event": event_type,
                "job_id": job_id,
                "status": status,
                "summary": summary[:200] if summary else "",
                "error": error[:200] if error else "",
            }
            payload = json.dumps(pkt, ensure_ascii=False)

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, sock.sendto,
                payload.encode("utf-8"), ("<broadcast>", self.udp_port),
            )
            logger.info(
                f"[SubagentCaller] UDP广播已发送 | "
                f"event={event_type} job_id={job_id} status={status}"
            )
        except Exception as e:
            logger.warning(f"[SubagentCaller] 发送UDP广播失败: {e}")
        finally:
            if sock:
                sock.close()

    # ─────────────────────────────────────────────
    # LLM唤醒核心逻辑
    # ─────────────────────────────────────────────

    async def _wake_up_llm(self, notification_text: str, session_id: str | None = None, job_id: str = "") -> None:
        """用真实会话 + CronMessageEvent + build_main_agent 唤醒主 agent。

        使用原会话构建 CronMessageEvent，通过 build_main_agent 重建主 agent
        （含完整工具链和 persona），然后运行 agent 处理通知。
        agent 可选择回复用户、派发新子代理任务、或不做决策（此时发送降级通知）。

        Args:
            notification_text: 通知内容。
            session_id: 会话 ID。传参时使用此 ID，不传则回退到 self._last_session_id。
            job_id: 可选，任务 ID，用于降级通知。
        """
        sid = session_id or self._last_session_id
        if not sid:
            logger.warning("[SubagentCaller] 无有效的原会话ID，无法唤醒LLM")
            return

        async with self._wake_lock:
            try:
                session = MessageSession.from_str(sid)
                cron_event = CronMessageEvent(
                    context=self.context,
                    session=session,
                    message=notification_text,
                    extras={},
                    message_type=session.message_type,
                )

                conv = await _get_session_conv(event=cron_event, plugin_context=self.context)

                req = ProviderRequest()
                req.conversation = conv
                req.prompt = notification_text

                if req.func_tool is None:
                    req.func_tool = ToolSet()
                config = MainAgentBuildConfig(tool_call_timeout=self.tool_call_timeout)

                logger.info(
                    f"[SubagentCaller] 正在通过真实会话唤醒 LLM: "
                    f"{notification_text[:80]}"
                )

                result = await build_main_agent(
                    event=cron_event,
                    plugin_context=self.context,
                    config=config,
                    req=req,
                )
                if not result:
                    logger.error("[SubagentCaller] build_main_agent 失败，无法唤醒LLM")
                    return

                runner = result.agent_runner
                async for _ in runner.step_until_done(self.max_steps):
                    pass

                llm_resp = runner.get_final_llm_resp()
                reply_text = llm_resp.completion_text.strip() if llm_resp and llm_resp.completion_text else ""

                dispatched = False
                for m in runner.run_context.messages:
                    role = getattr(m, 'role', None) or ''
                    if role == 'assistant':
                        tc = getattr(m, 'tool_calls', None) or []
                        for call in (tc if isinstance(tc, list) else []):
                            if isinstance(call, dict):
                                func = call.get('function', {})
                                name = func.get('name', '') if isinstance(func, dict) else ''
                            else:
                                name = getattr(getattr(call, 'function', None), 'name', '') or ''
                            if name == 'call_subagent':
                                dispatched = True
                                break
                    if not dispatched:
                        content = getattr(m, 'content', None) or ''
                        if "任务已派发给" in content:
                            dispatched = True
                    if dispatched:
                        break

                if reply_text and not dispatched:
                    logger.info(
                        f"[SubagentCaller] 后台唤醒处理完毕 ({len(reply_text)}字)，"
                        f"agent 选择汇报用户"
                    )
                    try:
                        from astrbot.api.message_components import Plain
                        await cron_event.send(Plain(text=reply_text))
                    except Exception as e:
                        logger.warning(f"[SubagentCaller] 发送回复失败: {e}")
                elif dispatched:
                    logger.info("[SubagentCaller] agent 选择转派新任务，跳过发消息")
                else:
                    logger.warning("[SubagentCaller] agent 未做决策，发送降级通知")
                    try:
                        from astrbot.api.message_components import Plain
                        jid_ref = job_id or "(未知)"
                        fallback = (
                            f"子代理任务 {jid_ref} 已完成，但主代理未选择汇报或转派。\n"
                            f"使用 call_subagent action=fetch job_id={jid_ref} 查看结果。"
                        )
                        await cron_event.send(Plain(text=fallback))
                    except Exception as e:
                        logger.warning(f"[SubagentCaller] 发送降级通知失败: {e}")

                try:
                    from astrbot.core.agent.message import (
                        UserMessageSegment, AssistantMessageSegment, TextPart,
                    )
                    conv_id = await self.context.conversation_manager.get_curr_conversation_id(sid)
                    if conv_id:
                        note_msg = f"[后台] 子代理任务结果已处理: {notification_text[:200]}"
                        decision_msg = reply_text or f"(agent 未决策，已发送降级通知)"
                        user_msg = UserMessageSegment(content=[TextPart(text=note_msg)])
                        assistant_msg = AssistantMessageSegment(content=[TextPart(text=decision_msg)])
                        await self.context.conversation_manager.add_message_pair(
                            cid=conv_id,
                            user_message=user_msg,
                            assistant_message=assistant_msg,
                        )
                except Exception as e:
                    logger.warning(f"[SubagentCaller] 注入对话历史失败: {e}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(
                    f"[SubagentCaller] 唤醒LLM异常: {e}", exc_info=True
                )

    # ─────────────────────────────────────────────
    # 子代理任务完成通知 → 后台唤醒（真实会话，agent自主决策）
    # ─────────────────────────────────────────────
    async def _notify_llm(
        self,
        event: AstrMessageEvent,
        jid: str,
        status_icon: str,
        status_text: str,
        result: str,
    ) -> None:
        """子代理任务完成后通知主 LLM。

        保存会话上下文并调用 _wake_up_llm 通过 build_main_agent 重建主 agent。
        主 agent 可选择：
        - 回复用户
        - 通过 call_subagent 派发新任务
        """
        # 记录日志
        try:
            preview = result[:200].replace("\n", " ")
            if len(result) > 200:
                preview += "..."
            logger.info(
                f"[SubagentCaller] 子代理任务 {jid} {status_text} | "
                f"结果摘要: {preview}"
            )
        except Exception as e:
            logger.error(f"[SubagentCaller] 记录通知日志失败: {e}")

        # 后台唤醒 LLM（独立虚拟会话，不抢前台锁）
        notify_ok = False
        if self.notify_on_complete:
            try:
                preview = result[:300].replace("\n", " ")
                if len(result) > 300:
                    preview += "..."

                # 防止子代理输出中的XML/标记注入到agent指令中
                safe_preview = preview.replace("<", "＜").replace(">", "＞")
                notification = (
                    f"＜system_instruction＞子代理任务 {jid} {status_text}。"
                    f"任务结果摘要：{safe_preview}\n\n"
                    f"当前对话中可能还有用户后续补充的指示，请结合完整对话历史判断如何处理。\n"
                    f"请选择以下方式之一处理：\n"
                    f"1. 通知用户 → 直接输出你的回复内容，系统会自动发送给用户\n"
                    f"2. 继续转派 → 使用 call_subagent action=dispatch，系统会处理转派\n\n"
                    f"注意：必须选择一种方式，不允许不做任何操作。"
                    f"无需调用 send_message_to_user。＜/system_instruction＞"
                )

                # 防止 _wake_lock 死锁：如果锁已被当前协程持有则跳过
                if self._wake_lock.locked():
                    logger.warning(f"[SubagentCaller] _wake_lock 已被占用，跳过重复唤醒 job_id={jid}")
                else:
                    await self._wake_up_llm(notification, session_id=event.unified_msg_origin, job_id=jid)
                    notify_ok = True

            except Exception as e:
                logger.error(f"[SubagentCaller] 后台唤醒失败: {e}")

        # 标记此 job 已由 _notify_llm 处理，防止 UDP 重复唤醒。
        # 仅在 _wake_up_llm 成功（或 notify_on_complete 关闭）时标记，
        # 确保失败时 UDP 广播能作为备用唤醒路径。
        if not self.notify_on_complete or notify_ok:
            async with self._task_lock:
                self._notified_jobs.add(jid)
                if len(self._notified_jobs) > self._max_notified:
                    to_discard = len(self._notified_jobs) - self._max_notified // 2
                for old in list(self._notified_jobs)[:to_discard]:
                    self._notified_jobs.discard(old)

    async def _save_session_context(self, event: AstrMessageEvent) -> None:
        """保存当前会话上下文，供后续唤醒使用。"""
        try:
            self._last_session_id = event.unified_msg_origin
            self._last_provider_id = await self.context.get_current_chat_provider_id(
                event.unified_msg_origin
            )
        except Exception as e:
            logger.debug(f"[SubagentCaller] 保存会话上下文失败: {e}")

    # ─────────────────────────────────────────────
    # 子代理执行逻辑（保持不变）
    # ─────────────────────────────────────────────

    def _get_handoff_agents(self) -> dict[str, tuple[str, Any, Any]]:
        orch = getattr(self.context, "subagent_orchestrator", None)
        if not orch:
            return {}
        result = {}
        for handoff in getattr(orch, "handoffs", []) or []:
            ag = getattr(handoff, "agent", None)
            if not ag:
                continue
            name = getattr(ag, "name", None)
            if isinstance(name, str) and name.strip():
                result[name.strip().casefold()] = (name.strip(), ag, handoff)
        return result

    async def _run_subagent(
        self,
        event: AstrMessageEvent,
        request_text: str,
        agent_name: str,
        max_steps: int,
        tool_call_timeout: int,
    ) -> str:
        agents = self._get_handoff_agents()
        key = agent_name.strip().casefold()
        if key not in agents:
            names = [v[0] for v in agents.values()]
            hint = f"，可选: {', '.join(names)}" if names else "，请先配置子代理编排"
            return f"子代理 '{agent_name}' 未找到{hint}。"

        resolved_name, agent, handoff = agents[key]

        astr_ctx = AstrAgentContext(context=self.context, event=event)
        rc = AgentContextWrapper(
            context=astr_ctx, tool_call_timeout=tool_call_timeout
        )

        tools_arg = None if self.inherit_all_tools else agent.tools
        toolset = FunctionToolExecutor._build_handoff_toolset(rc, tools_arg)

        # 补充所有插件 LLM 工具（如 web_search_tavily 等 @filter.llm_tool 注册的工具）
        if self.inherit_all_tools:
            try:
                plugin_mgr = getattr(self.context, 'plugin_manager', None)
                plugins: list = []
                if plugin_mgr:
                    if hasattr(plugin_mgr, 'get_plugins') and callable(plugin_mgr.get_plugins):
                        plugins = plugin_mgr.get_plugins() or []
                    elif hasattr(plugin_mgr, 'plugins'):
                        plugins = plugin_mgr.plugins or []
                existing_names = {t.name for t in (toolset.tools or [])}
                for p in plugins:
                    handlers = list(getattr(p, 'handlers', None) or [])
                    for h in handlers:
                        htype = str(getattr(h, 'handler_type', '') or '')
                        if 'llm_tool' in htype.casefold():
                            tname = getattr(h, 'tool_name', None) or getattr(h, 'name', None)
                            if tname and tname not in existing_names:
                                existing_names.add(tname)
                                ft = FunctionTool(
                                    name=tname,
                                    description=str(getattr(h, 'description', '') or ''),
                                    parameters=getattr(h, 'params', {}) or {},
                                    handler=getattr(h, 'handler', None) or h,
                                )
                                toolset.add_tool(ft)
            except Exception as e:
                logger.warning(f"[SubagentCaller] 继承LLM工具失败: {e}")

        # 注入shell执行工具 - 只有通过插件dispatch的子代理才能拿到这个权限
        if toolset is None:
            toolset = ToolSet()
        toolset.add_tool(_SHELL_TOOL)
        # 注入子代理间通信工具 - call_peer，让子代理可以互相委派
        toolset.add_tool(self._peer_tool)

        prov_id = getattr(
            handoff, "provider_id", None
        ) or await self.context.get_current_chat_provider_id(event.unified_msg_origin)
        provider = self.context.get_provider_by_id(prov_id)
        if not provider:
            return f"未找到子代理 '{resolved_name}' 的 provider: {prov_id}"

        exec_prompt = """\n

【权限说明】
当前子代理具有 shell 执行权限，可使用 `subagent_shell_exec` 工具在服务器上执行系统命令（包括通过 sshpass SSH 连接其他服务器）。
仅执行任务范围内的操作。

【协作能力】
可使用 `call_peer` 工具将子任务委派给其他子代理协作完成。
适用场景示例：
- 任务可拆分为独立子任务，需不同专长的子代理处理
- 当前子代理缺少完成特定子任务所需的工具或权限
- 其他子代理拥有更合适的领域知识或数据源
不可委派给已在当前委派链中的子代理（防止循环）。

【执行规则】
1. 按照调用你的 Agent 的指示逐步调用工具完成任务。
2. 仅执行你的 Agent 要求的操作，不自行添加额外操作。
3. 不分析或总结无关内容，执行后直接返回文本结果。
4. 任务明确要求只读（如查状态、看日志）时，不执行修改操作。
5. 任务明确要求修改（如重启、写入）时，按指示执行。
6. 执行完毕后返回格式整洁的结果摘要。
7. 指令不明确或权限不确定时，返回给调用你的 Agent 去决策。
"""
        base_system_prompt = (agent.instructions or "") + exec_prompt

        request = ProviderRequest(
            prompt=request_text,
            func_tool=toolset,
            system_prompt=base_system_prompt,
            session_id=event.unified_msg_origin,
        )

        runner = ToolLoopAgentRunner()
        try:
            await runner.reset(
                provider=provider,
                request=request,
                run_context=rc,
                tool_executor=FunctionToolExecutor(),
                agent_hooks=BaseAgentRunHooks(),
                streaming=False,
            )

            step_count = 0
            while not runner.done() and step_count < max_steps:
                step_count += 1
                async for _ in runner.step():
                    pass

            if not runner.done():
                if runner.req:
                    runner.req.func_tool = None
                runner.run_context.messages.append(
                    Message(
                        role="user",
                        content="工具调用次数已达到上限，请根据已经收集到的信息进行总结并回复。",
                    )
                )
                async for _ in runner.step():
                    pass

            llm_resp = runner.get_final_llm_resp()
            if llm_resp and llm_resp.completion_text:
                return llm_resp.completion_text.strip()
            return f"'{resolved_name}' 执行完毕，未返回文本内容。"
        except asyncio.CancelledError:
            logger.info(f"[SubagentCaller] {resolved_name} 任务被取消")
            raise
        except Exception as e:
            logger.error(
                f"[SubagentCaller] {resolved_name} 异常: {e}", exc_info=True
            )
            return f"'{resolved_name}' 执行异常: {e}"
        finally:
            try:
                await runner.cleanup()
            except Exception:
                pass

    # ─────────────────────────────────────────────
    # 子代理间委派通信（v1.7.0）
    # ─────────────────────────────────────────────

    async def _on_call_peer(self, event, agent_name: str, request_text: str, max_steps: int = -1) -> str:
        """FunctionTool 桥接：将 LLM 发起的工具调用转发到 _call_peer。"""
        return await self._call_peer(event, agent_name, request_text, max_steps)

    async def _call_peer(
        self,
        event: AstrMessageEvent,
        agent_name: str,
        request_text: str,
        max_steps: int = -1,
    ) -> str:
        """子代理之间的委派调用。

        子代理 A 可通过此工具将子任务委派给子代理 B 执行，并等待 B 返回结果。
        支持多级链式委派（A→B→C），内置循环检测和深度保护。

        Args:
            event: 透传的 LLM 工具事件。
            agent_name: 目标子代理名称。
            request_text: 要委派的任务描述。
            max_steps: 目标子代理最大步数，-1 使用插件默认。

        Returns:
            目标子代理的执行结果文本，或错误描述。
        """
        # 规范 max_steps：非正值使用插件默认
        if max_steps <= 0:
            max_steps = self.max_steps
        max_steps = max(1, max_steps)

        # 1. 深度检查
        current_depth = getattr(event, '_subagent_depth', 0)
        if current_depth >= self.max_delegation_depth:
            path = getattr(event, '_subagent_path', [])
            logger.warning(
                f"[SubagentCaller] 委派深度已达上限 ({self.max_delegation_depth}): "
                f"{' → '.join(path)} → {agent_name}"
            )
            chain_str = ' → '.join(path)
            return (
                f"❌ 委派深度已达上限 ({self.max_delegation_depth})，"
                f"无法继续委派给 '{agent_name}'。\n"
                f"当前委派链: {chain_str} → {agent_name}\n"
                f"请直接处理任务，不要再继续委派。"
            )

        # 2. 循环委派检测
        path: list = getattr(event, '_subagent_path', [])
        if agent_name in path:
            logger.warning(
                f"[SubagentCaller] 检测到循环委派: "
                f"{' → '.join(path)} → {agent_name} (循环!)"
            )
            return (
                f"❌ 检测到循环委派，'{agent_name}' 已经在当前委派链中。\n"
                f"委派链: {' → '.join(path)} → {agent_name}\n"
                f"请选择其他子代理或直接处理任务。"
            )

        # 3. 保存原始深度跟踪，执行递归委派
        steps = max_steps
        tool_timeout = self.tool_call_timeout
        original_depth = current_depth
        original_path = list(path)

        event._subagent_depth = original_depth + 1
        event._subagent_path = original_path + [agent_name]

        chain_str = ' → '.join(event._subagent_path)

        logger.info(
            f"[SubagentCaller] 子代理间委派: {chain_str} | "
            f"depth={current_depth + 1}/{self.max_delegation_depth} | "
            f"max_steps={steps} | tool_timeout={tool_timeout}s"
        )

        try:
            result = await self._run_subagent(
                event=event,
                request_text=request_text,
                agent_name=agent_name,
                max_steps=steps,
                tool_call_timeout=tool_timeout,
            )
            logger.info(
                f"[SubagentCaller] 子代理间委派完成: {chain_str} | "
                f"结果长度={len(result)}"
            )
            return result
        except asyncio.CancelledError:
            logger.info(f"[SubagentCaller] 子代理间委派被取消: {chain_str}")
            raise
        except Exception as e:
            logger.error(
                f"[SubagentCaller] 子代理间委派失败: {chain_str}: {e}",
                exc_info=True,
            )
            return (
                f"❌ 委派给 '{agent_name}' 时发生异常: {e}\n"
                f"请自行处理任务或选择其他子代理。"
            )
        finally:
            # 无论成功/失败/异常，恢复原始深度跟踪，防止状态污染
            event._subagent_depth = original_depth
            event._subagent_path = original_path

    async def _finalize_task(
        self,
        event: AstrMessageEvent,
        jid: str,
        result: str,
        status_icon: str = "",
        status_text: str = "",
        udp_status: str = "",
        wake_llm: bool = True,
    ) -> None:
        """保存任务结果、通知 LLM 并发送 UDP 广播。"""
        async with self._task_lock:
            self._results[jid] = result
            self._completion_times[jid] = time.time()
            self._running_jobs.pop(jid, None)
            # 限制 _results 大小，防止内存泄漏
            while len(self._results) > self._max_results:
                oldest = min(self._completion_times, key=self._completion_times.get)
                self._results.pop(oldest, None)
                self._completion_times.pop(oldest, None)
                self._running_jobs.pop(oldest, None)
                self._task_briefs.pop(oldest, None)
                self._tasks.pop(oldest, None)
                self._notified_jobs.discard(oldest)

        if not self.auto_notify:
            return

        if wake_llm:
            await self._notify_llm(
                event=event, jid=jid,
                status_icon=status_icon, status_text=status_text,
                result=result or "",
            )
        await self._send_udp_broadcast(
            event_type="task_done", job_id=jid, status=udp_status,
            summary=result[:200] if result else "",
        )

    async def _run_and_store(
        self,
        event: AstrMessageEvent,
        request_text: str,
        agent_name: str,
        max_steps: int,
        tool_call_timeout: int,
        jid: str,
        task_timeout: int = 120,
    ) -> None:
        """在后台执行子代理任务，完成后保存结果并通知 LLM。"""
        await self._save_session_context(event)

        try:
            result = await asyncio.wait_for(
                self._run_subagent(
                    event, request_text, agent_name, max_steps, tool_call_timeout
                ),
                timeout=task_timeout,
            )
            await self._finalize_task(event, jid, result, udp_status="completed")
        except asyncio.TimeoutError:
            err_msg = f"任务 {jid} 超时（超过 {task_timeout}s）。"
            await self._finalize_task(event, jid, err_msg,
                                      status_icon="⚠️", status_text="超时了", udp_status="timeout")
        except asyncio.CancelledError:
            err_msg = f"任务 {jid} 已被取消。"
            await self._finalize_task(event, jid, err_msg,
                                      udp_status="cancelled", wake_llm=False)
        except Exception as e:
            logger.error(f"[SubagentCaller] 后台任务 {jid} 异常: {e}", exc_info=True)
            err_msg = f"任务 {jid} 执行异常: {e}"
            await self._finalize_task(event, jid, err_msg,
                                      status_icon="❌", status_text="出错了", udp_status="error")

    # ─────────────────────────────────────────────
    # call_subagent 工具接口（保持不变）
    # ─────────────────────────────────────────────

    @filter.llm_tool(name="call_subagent")
    async def call_subagent(
        self,
        event: AstrMessageEvent,
        action: str,
        request_text: str = "",
        agent_name: str = "",
        max_steps: int = -1,
        job_id: str = "",
        brief: str = "",
    ) -> str:
        """调度子代理执行任务。支持多任务轮询调度：

        注意 — dispatch 是异步操作：
        - 派发后立即返回 job_id，任务在后台执行，不阻塞当前对话
        - 派发后继续回复用户或处理其他事务，无需等待
        - 任务完成后系统会自动唤醒 agent，届时使用 fetch 获取结果
        - 不要重复 dispatch 相同任务

        action 列表：
        dispatch=派发后台任务（立即返回 job_id），
        status=查看所有任务状态，
        fetch=获取已完成任务的完整结果，
        cancel=取消指定任务，
        stop=停止（暂未实现）。

        Args:
            action(string): 必填。dispatch/status/fetch/cancel/stop。
            request_text(string): dispatch 时必填，子代理要执行的任务描述。
            agent_name(string): 可选，dispatch 时指定目标子代理，留空自动选择。
            max_steps(number): 可选，dispatch 时指定最大工具调用轮数。
            job_id(string): fetch/cancel 时必填，指定任务 ID。
            brief(string): 可选，dispatch 时简短任务描述（最多10字），用于快速识别。
        """
        await self._save_session_context(event)

        act = (action or "").strip().casefold()

        # === dispatch: 派发后台任务，立即返回 job_id ===
        if act == "dispatch":
            if not request_text.strip():
                return "dispatch 时 request_text 不能为空。"

            # 清理残留记录（结果已被 fetch 取走但 _results 仍保留的旧条目）
            async with self._task_lock:
                orphan_results = [jid for jid in self._results if jid not in self._tasks]
                for jid in orphan_results:
                    self._results.pop(jid, None)
                    self._running_jobs.pop(jid, None)
                    self._completion_times.pop(jid, None)
                    self._task_briefs.pop(jid, None)
                    self._notified_jobs.discard(jid)

            agents = self._get_handoff_agents()
            if not agents:
                return "没有可用的子代理，请先在配置中启用子代理编排。"

            target = agent_name.strip()
            if not target:
                target = next(
                    (a for a in [self.agent_a, self.agent_b, self.agent_c]
                     if a and a.casefold() in agents),
                    None,
                ) or next(iter(agents.values()))[0]
            else:
                key = target.casefold()
                if key not in agents:
                    names = [v[0] for v in agents.values()]
                    return f"子代理 '{target}' 未找到。可选: {', '.join(names)}"

            steps = max_steps if max_steps > 0 else self.max_steps

            # brief 截取10字以内
            brief_text = (brief.strip()[:10] if brief else "")

            async with self._task_lock:
                self._task_counter += 1
                jid = f"job_{self._task_counter}"

            logger.info(
                f"[SubagentCaller] dispatch -> {target} | "
                f"job_id={jid} | brief='{brief_text}' | max_steps={steps} | timeout={self.tool_call_timeout}s"
            )

            task = asyncio.create_task(
                self._run_and_store(
                    event, request_text, target, steps, self.tool_call_timeout, jid,
                    task_timeout=self.task_timeout,
                )
            )

            async with self._task_lock:
                self._tasks[jid] = task
                self._running_jobs[jid] = time.time()
                self._task_briefs[jid] = brief_text

            ts = time.strftime("%H:%M:%S", time.localtime(self._running_jobs[jid]))
            notify_hint = "（完成后会自动唤醒我）" if self.notify_on_complete else "（完成后会记录日志）"
            async with self._task_lock:
                other_tasks = [t for t in self._tasks if t != jid and not self._tasks[t].done()]
            extra_line = "（已有其他在途任务，系统会自动监测）\n" if other_tasks else ""
            return (
                f"✅ 任务已派发给 '{target}'，job_id={jid} ({ts}) {notify_hint}\n"
                f"{extra_line}"
                f"请继续回答用户或处理其他事务，任务完成后会自动唤醒你。\n"
                f"届时使用 call_subagent action=fetch job_id={jid} 获取结果。"
            )

        # === status: 查看所有任务状态 ===
        elif act == "status":
            async with self._task_lock:
                if not self._tasks:
                    return "当前没有运行中的任务。"

                lines = ["📋 当前任务状态："]
                for jid, task in self._tasks.items():
                    ts = ""
                    if jid in self._running_jobs:
                        ts = time.strftime("%H:%M:%S", time.localtime(self._running_jobs[jid]))
                    if task.done():
                        has_result = jid in self._results
                        status = "✅ 已完成" if has_result else "⚠️ 已完成(异常)"
                    else:
                        elapsed = time.time() - self._running_jobs.get(jid, time.time())
                        status = f"⏳ 运行中 ({elapsed:.0f}s)"

                    brief_part = f" [{self._task_briefs[jid]}]" if self._task_briefs.get(jid) else ""
                    ts_part = f"  [{ts}]" if ts else ""
                    lines.append(f"  {jid}{brief_part}: {status}{ts_part}")

                if self._results:
                    lines.append("")
                    lines.append("💡 使用 call_subagent action=fetch job_id=<id> 获取结果")

                return "\n".join(lines)

        # === fetch: 获取指定任务的结果 ===
        elif act == "fetch":
            if not job_id.strip():
                return "fetch 时 job_id 不能为空。"

            async with self._task_lock:
                if job_id in self._tasks and not self._tasks[job_id].done():
                    elapsed = time.time() - self._running_jobs.get(job_id, time.time())
                    return f"⏳ job_id={job_id} 仍在执行中，请稍后再查（已运行 {elapsed:.0f}s）。"
                if job_id not in self._results and job_id not in self._tasks:
                    return f"job_id '{job_id}' 不存在（结果已被取走或从未派发）。"

                result = self._results.pop(job_id, None)
                self._tasks.pop(job_id, None)
                run_ts = self._running_jobs.pop(job_id, None)
                comp_ts = self._completion_times.pop(job_id, None)
                brief = self._task_briefs.pop(job_id, "")
                self._notified_jobs.discard(job_id)

            ts_parts = []
            if run_ts:
                ts_parts.append(f"派发: {time.strftime('%H:%M:%S', time.localtime(run_ts))}")
            if comp_ts:
                ts_parts.append(f"完成: {time.strftime('%H:%M:%S', time.localtime(comp_ts))}")
            ts_str = f" [{' | '.join(ts_parts)}]" if ts_parts else ""

            if result is None:
                return f"⚠️ job_id={job_id} 已完成但无返回结果。{ts_str}"

            async with self._task_lock:
                has_other = len(self._tasks) > 0
            brief_label = f" [{brief}]" if brief else ""
            suffix = "\n（仍有其他在途任务，系统自动监测中）" if has_other else ""
            return f"📄 job_id={job_id}{brief_label}{ts_str} 结果：\n{result}{suffix}"

        # === cancel: 取消指定任务 ===
        elif act == "cancel":
            if not job_id.strip():
                return "cancel 时 job_id 不能为空。"

            async with self._task_lock:
                if job_id in self._tasks:
                    task = self._tasks.pop(job_id, None)
                    self._running_jobs.pop(job_id, None)
                    self._results.pop(job_id, None)
                    self._task_briefs.pop(job_id, None)
                    if task:
                        task.cancel()
                    return f"🛑 job_id={job_id} 已取消。"
                elif job_id in self._results:
                    self._results.pop(job_id, None)
                    return f"🛑 job_id={job_id} 结果已清除。"
                else:
                    return f"job_id '{job_id}' 不存在。"

        elif act == "stop":
            return "当前版本 stop 暂未实现，任务会自然结束。"

        else:
            return "action 仅支持 dispatch/status/fetch/cancel。"
