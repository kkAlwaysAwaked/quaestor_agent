"""聊天 Stream 运行时：原子写事件、隔离 attempt，并在终态后设置保留期。"""

from __future__ import annotations

import asyncio
import json

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

from core.chat_contracts import AgentError, ChatStreamEvent, LeaseLost, chat_stream_key
from core.config import AgentSettings, RedisRuntimeSettings


# START 不裁剪原 Stream；新 attempt 的 reset 事件让接收方清空旧尝试的临时答案。
START_SCRIPT = """
local old = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0')
local attempt = tonumber(ARGV[1])
if old > attempt or redis.call('HGET', KEYS[2], 'terminal') == '1' then return 0 end
if old == attempt and redis.call('HGET', KEYS[2], 'owner') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[2], 'attempt', ARGV[1], 'owner', ARGV[2], 'terminal', '0')
local id = redis.call('XADD', KEYS[1], '*', 'event', 'status', 'data', ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[4])
return id
"""

APPEND_SCRIPT = """
if redis.call('HGET', KEYS[2], 'attempt') ~= ARGV[1] or
   redis.call('HGET', KEYS[2], 'owner') ~= ARGV[2] or
   redis.call('HGET', KEYS[2], 'terminal') == '1' then return 0 end
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
local id = redis.call('XADD', KEYS[1], '*', 'event', ARGV[3], 'data', ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[5])
return id
"""

TOUCH_SCRIPT = """
if redis.call('HGET', KEYS[2], 'attempt') ~= ARGV[1] or
   redis.call('HGET', KEYS[2], 'owner') ~= ARGV[2] or
   redis.call('HGET', KEYS[2], 'terminal') == '1' then return 0 end
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3])
return 1
"""

TERMINAL_SCRIPT = """
local existing = redis.call('HGET', KEYS[2], 'event_id')
if existing == ARGV[3] then return redis.call('HGET', KEYS[2], 'entry_id') end
local old = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0')
if old > tonumber(ARGV[1]) or redis.call('HGET', KEYS[2], 'terminal') == '1' then return 0 end
local id = redis.call('XADD', KEYS[1], '*', 'event', ARGV[2], 'data', ARGV[4])
redis.call('HSET', KEYS[2], 'attempt', ARGV[1], 'terminal', '1', 'event_id', ARGV[3], 'entry_id', id)
redis.call('EXPIRE', KEYS[1], ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[5])
return id
"""


# 作用：创建有连接与读写时限的 Redis 客户端，把重试交给任务和 Outbox 编排。
def create_redis_client(settings: RedisRuntimeSettings, *, timeout_seconds: int = 5) -> Redis:
    return Redis(
        host=settings.host, port=settings.port, password=settings.password,
        decode_responses=True, socket_connect_timeout=timeout_seconds,
        socket_timeout=timeout_seconds, retry=Retry(NoBackoff(), 0),
        max_connections=10, health_check_interval=30,
    )


class ChatStreams:
    # 作用：绑定 Redis 连接与保留配置，不在构造时发起网络请求。
    def __init__(self, client: Redis, settings: AgentSettings) -> None:
        self.client = client
        self.settings = settings

    # 作用：在有限时限内原子执行写入脚本，统一报告安全的 Redis 故障码。
    async def _eval(self, script: str, request_id: str, *args):
        key = chat_stream_key(request_id)
        try:
            async with asyncio.timeout(self.settings.io_timeout_seconds):
                return await self.client.eval(script, 2, key, f"{key}:state", *args)
        except (RedisError, OSError, TimeoutError) as exc:
            raise AgentError("redis_unavailable", "实时事件服务暂不可用") from exc

    # 作用：写新尝试的 reset 状态，其他活动事件仅允许当前 attempt 与 owner 追加。
    async def emit_active(self, event: ChatStreamEvent, *, owner: str) -> str:
        if event.event not in ("status", "token"):
            raise ValueError("活动写入只能是 status/token")
        request_id, attempt = event.data["request_id"], event.data["attempt"]
        data = json.dumps(event.data, ensure_ascii=False, separators=(",", ":"))
        if event.event == "status" and event.data.get("phase") == "started" and event.data.get("reset") is True:
            result = await self._eval(START_SCRIPT, request_id, attempt, owner, data, self.settings.active_stream_ttl_seconds)
        else:
            result = await self._eval(APPEND_SCRIPT, request_id, attempt, owner, event.event, data, self.settings.active_stream_ttl_seconds)
        if result == -1:
            raise AgentError("stream_lost", "实时事件记录丢失，需要新尝试重置")
        if not result:
            raise LeaseLost()
        return str(result)

    # 作用：心跳延长活动流保留时间，终态流与旧 attempt 都不能再续期。
    async def touch_active(self, *, request_id: str, attempt: int, owner: str) -> None:
        result = await self._eval(TOUCH_SCRIPT, request_id, attempt, owner, self.settings.active_stream_ttl_seconds)
        if result == -1:
            raise AgentError("stream_lost", "实时事件记录丢失，需要新尝试重置")
        if not result:
            raise LeaseLost()

    # 作用：发布已提交数据库的终态并设置保留期，以 Outbox ID 去重不确定的重复发送。
    async def emit_terminal(self, event: ChatStreamEvent) -> str:
        if event.event not in ("done", "error") or not event.data.get("event_id"):
            raise ValueError("终态必须为 done/error 且包含 event_id")
        result = await self._eval(
            TERMINAL_SCRIPT, event.data["request_id"], event.data["attempt"], event.event,
            event.data["event_id"], json.dumps(event.data, ensure_ascii=False, separators=(",", ":")),
            self.settings.terminal_stream_ttl_seconds,
        )
        if not result:
            raise AgentError("terminal_stream_conflict", "终态事件与已保存的执行次数不一致")
        return str(result)

    # 作用：按 Redis entry ID 读取完整事件，供基线脚本及后续网关的 SSE 续读使用。
    async def read(self, request_id: str, *, last_id: str = "0-0", block_ms: int = 1000, count: int = 100) -> list[tuple[str, ChatStreamEvent]]:
        key = chat_stream_key(request_id)
        async with asyncio.timeout(self.settings.io_timeout_seconds):
            batches = await self.client.xread({key: last_id}, count=count, block=block_ms)
        events = []
        for _, entries in batches:
            for entry_id, fields in entries:
                event = ChatStreamEvent(event=fields["event"], data=json.loads(fields["data"]))
                if event.data["request_id"] != request_id:
                    raise ValueError("Stream 事件请求归属不一致")
                events.append((entry_id, event))
        return events
