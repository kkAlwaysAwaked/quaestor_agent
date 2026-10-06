"""Agent 的真实分片编排、事务/确认边界与 Redis Lua 脚本离线验证。"""

from __future__ import annotations

import asyncio
import copy
import json
import tempfile
import unittest
from collections import deque
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import fakeredis
import httpx
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import create_async_engine

from core.chat_contracts import AgentError, ChatJob, ChatStreamEvent, LeaseLost, chat_stream_key
from core.config import AgentSettings, ServiceTokenSettings, TaskLeaseSettings, load_agent_settings, load_retrieval_token_settings
from core.retrieval_contracts import RetrieveRequest, RetrieveResponse, build_retrieve_response
from core.service_auth import verify_retrieval_token
from infra.mysql.base import Base, new_id, utc_now
from infra.mysql.models import ChatRequest, Conversation, Message, OutboxEvent, RetrievalRun, User
from infra.mysql.repositories.chat import accept_chat, finish_chat, load_chat_context, recover_expired_chats
from infra.mysql.repositories.tasks import claim_task
from infra.mysql.session import create_session_factory
from infra.redis_streams import ChatStreams
from services.agent.delivery import handle_message
from services.agent.outbox import publish_once
from services.agent.retrieval_client import RetrievalClient
from services.agent.worker import AgentRuntime, consume_messages, drain_tasks


# 作用：构造只包含流式协议字段的模型分片，不调用真实生成 API。
def chunk(*, content=None, reasoning=None, calls=None, finish=None):
    return SimpleNamespace(choices=[SimpleNamespace(
        delta=SimpleNamespace(content=content, reasoning_content=reasoning, tool_calls=calls, refusal=None),
        finish_reason=finish,
    )])


# 作用：构造一个工具 ID、名称或 JSON 参数的分片。
def tool_fragment(index: int, *, call_id=None, name=None, arguments=None):
    return SimpleNamespace(index=index, id=call_id, function=SimpleNamespace(name=name, arguments=arguments))


# 作用：构造分成两段的真实公开回答，最后带明确的 stop 结束标志。
def answer_turn(first="年假", second="十天") -> list:
    return [chunk(content=first), chunk(content=second, finish="stop")]


# 作用：构造尚未完成的 JSON 分片，以及同轮重复 RAG 调用。
def tool_turn(query="员工手册 年假 天数", *, duplicate=False) -> list:
    encoded = json.dumps({"query": query}, ensure_ascii=False)
    middle = len(encoded) // 2
    first = [tool_fragment(0, call_id="call_", name="RA", arguments=encoded[:middle])]
    last = [tool_fragment(0, call_id="a", name="G", arguments=encoded[middle:])]
    if duplicate:
        first.append(tool_fragment(1, call_id="call_b", name="RAG", arguments='{"query":"不同关键词"}'))
    return [chunk(reasoning="内部思考不能外泄", calls=first), chunk(calls=last, finish="tool_calls")]


class FakeStream:
    # 作用：为测试提供可暂停、可中断的异步模型流。
    def __init__(self, chunks: list, *, pause_at=None, gate=None) -> None:
        self.chunks = deque(chunks)
        self.pause_at = pause_at
        self.gate = gate
        self.index = 0
        self.closed = False

    # 作用：把模型流自身作为异步迭代器返回。
    def __aiter__(self):
        return self

    # 作用：逐片输出，并允许在指定片段之前阻塞以验证首 token 已经回流。
    async def __anext__(self):
        if self.pause_at == self.index:
            await self.gate.wait()
        if not self.chunks:
            raise StopAsyncIteration
        self.index += 1
        item = self.chunks.popleft()
        if isinstance(item, BaseException):
            raise item
        await asyncio.sleep(0)
        return item

    # 作用：记录超时、取消及正常结束后模型流是否被关闭。
    async def close(self) -> None:
        self.closed = True


class FakeModel:
    # 作用：依次返回预定模型流并保留发给模型的消息，检查工具协议与恢复行为。
    def __init__(self, turns: list) -> None:
        self.turns = deque(turns)
        self.requests = []
        self.chat = SimpleNamespace(completions=self)

    # 作用：记录真实 stream 请求参数，不允许超出测试预定的模型调用数量。
    async def create(self, **kwargs):
        self.requests.append(copy.deepcopy(kwargs))
        if not self.turns:
            raise AssertionError("额外调用了生成模型")
        turn = self.turns.popleft()
        if isinstance(turn, Exception):
            raise turn
        return turn if isinstance(turn, FakeStream) else FakeStream(turn)


class StubRetrieval:
    # 作用：模拟 HTTP 检索的成功持久化与快照复用，区分 RPC 次数和实际计算次数。
    def __init__(self, sessions) -> None:
        self.sessions = sessions
        self.calls = []
        self.computations = 0

    # 作用：相同输入成功后读取保存资料，模拟第 5 阶段的 request_id 幂等边界。
    async def retrieve(self, user_id, payload):
        self.calls.append(payload.model_dump(mode="json"))
        async with self.sessions() as session, session.begin():
            task = await session.get(ChatRequest, str(payload.request_id))
            if task is None or task.user_id != user_id:
                raise AgentError("request_not_found", "测试任务无权访问", retryable=False)
            run = await session.scalar(select(RetrievalRun).where(RetrievalRun.request_id == task.id))
            if run is not None and run.input_data != payload.model_dump(mode="json"):
                raise AgentError("retrieval_input_conflict", "检索输入变化", retryable=False)
            if run is not None and run.status == "succeeded":
                return RetrieveResponse.model_validate(run.result_data)
            self.computations += 1
            result = build_retrieve_response(task.id, [{
                "parent_id": new_id(), "document_id": new_id(), "version_id": new_id(),
                "content": "正式员工每年享有十天带薪年假。", "source": "employee_handbook.md",
                "rrf_score": 0.03, "rerank_score": 1.0,
            }])
            if run is None:
                run = RetrievalRun(
                    request_id=task.id, user_id=user_id, search_query=payload.search_query,
                    input_data=payload.model_dump(mode="json"), attempt=0,
                )
                session.add(run)
            run.attempt += 1
            run.status = "succeeded"
            run.result_data = result.model_dump(mode="json")
            run.retrieved_parent_ids = result.retrieved_parent_ids
            run.source_version_ids = [result.documents[0].version_id]
            run.trace_data = {"stub": True}
        return result


class FakeExchange:
    # 作用：提供 publisher confirm 替身，支持明确拒绝发布的故障注入。
    def __init__(self) -> None:
        self.published = []
        self.confirmed = True

    # 作用：记录消息持久化、mandatory 和 event_id，返回可控的发布确认。
    async def publish(self, message, **kwargs):
        self.published.append((message, kwargs))
        return self.confirmed


class FakeMessage:
    # 作用：绑定队列正文和实际任务，使确认动作能够核对数据库是否已经提交。
    def __init__(self, task, runtime, *, body=None) -> None:
        self.task_id = task.id
        self.runtime = runtime
        self.body = body or ChatJob(schema_version=1, request_id=task.id, user_id=task.user_id, conversation_id=task.conversation_id).model_dump_json().encode()
        self.processed = False
        self.action = None
        self.status_at_confirmation = None

    # 作用：在消息确认瞬间读取另一个数据库会话，验证结果在确认之前可见。
    async def capture(self, action: str) -> None:
        async with self.runtime.sessions() as session:
            task = await session.get(ChatRequest, self.task_id)
            self.status_at_confirmation = task.status
        self.action = action
        self.processed = True

    # 作用：模拟消费端 ACK 并记录当时业务状态。
    async def ack(self):
        await self.capture("ack")

    # 作用：模拟最终失败或非法通知进入死信队列。
    async def reject(self, *, requeue):
        await self.capture("reject" if not requeue else "requeue")

    # 作用：模拟无法可靠提交时的 RabbitMQ 重投。
    async def nack(self, *, requeue):
        await self.capture("requeue" if requeue else "reject")


class AgentComponentTests(unittest.IsolatedAsyncioTestCase):
    # 作用：准备临时数据库和真正执行 Lua 的 Redis 替身，测试不调用真实模型 API。
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        path = (Path(self.temp.name) / "agent.sqlite").as_posix()
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{path}")

        # 作用：打开 SQLite 外键检查以捕获任务、消息和会话引用关系错误。
        @event.listens_for(self.engine.sync_engine, "connect")
        def enable_foreign_keys(connection, record):
            cursor = connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = create_session_factory(self.engine)
        self.user_id, self.conversation_id = new_id(), new_id()
        async with self.sessions() as session, session.begin():
            session.add(User(id=self.user_id, email="agent@example.invalid", password_hash="!"))
            await session.flush()
            session.add(Conversation(id=self.conversation_id, user_id=self.user_id))
        self.settings = AgentSettings()
        self.redis = fakeredis.FakeAsyncRedis(version=(7, 4, 11), decode_responses=True)
        self.retrieval = StubRetrieval(self.sessions)
        self.runtime = SimpleNamespace(
            sessions=self.sessions, settings=self.settings,
            lease=TaskLeaseSettings(120, 50, 50), retrieval=self.retrieval,
            streams=ChatStreams(self.redis, self.settings), exchange=FakeExchange(),
            model=FakeModel([tool_turn(duplicate=True), answer_turn()]),
        )
        self.task = await self.accept()

    # 作用：关闭 Redis 替身、数据库连接及临时文件，保持测试彼此独立。
    async def asyncTearDown(self):
        await self.redis.aclose()
        await self.engine.dispose()
        self.temp.cleanup()

    # 作用：经受理事务建立真实聊天任务及工作 Outbox。
    async def accept(self, content="我每年有多少天年假？", *, key=None):
        async with self.sessions() as session, session.begin():
            return await accept_chat(
                session, user_id=self.user_id, conversation_id=self.conversation_id,
                content=content, idempotency_key=key or new_id(),
            )

    # 作用：读取当前任务与已保存回答，便于检查提交状态。
    async def inspect_task(self):
        async with self.sessions() as session:
            task = await session.get(ChatRequest, self.task.id)
            answer = await session.get(Message, task.result_message_id) if task.result_message_id else None
            return task, answer

    # 作用：解码 Redis 中所有事件，检查结构化契约、attempt 和事件顺序。
    async def stream_events(self):
        entries = await self.redis.xrange(chat_stream_key(self.task.id))
        return [(entry_id, ChatStreamEvent(event=fields["event"], data=json.loads(fields["data"]))) for entry_id, fields in entries]

    # 作用：强制已安排重试到期，仅用于测试下一次执行不等待真实退避时间。
    async def make_retry_due(self):
        async with self.sessions() as session, session.begin():
            task = await session.get(ChatRequest, self.task.id)
            task.available_at = utc_now() - timedelta(seconds=1)

    # 作用：验证工具分片完整后才执行，内部推理不回流，答案提交后发布 done 再 ACK。
    async def test_streaming_tools_commit_and_ack(self):
        message = FakeMessage(self.task, self.runtime)
        await handle_message(message, self.runtime, owner="owner-a")
        task, answer = await self.inspect_task()
        self.assertEqual((task.status, answer.content, message.action), ("succeeded", "年假十天", "ack"))
        self.assertEqual(message.status_at_confirmation, "succeeded")
        self.assertEqual(self.retrieval.computations, 1)
        self.assertEqual(len(self.retrieval.calls), 1)
        self.assertEqual(self.retrieval.calls[0]["search_query"], "员工手册 年假 天数")
        tool_messages = [item for item in self.runtime.model.requests[1]["messages"] if item["role"] == "tool"]
        self.assertEqual(len(tool_messages), 2)
        self.assertEqual(tool_messages[0]["content"], tool_messages[1]["content"])
        self.assertIn("reasoning_content", self.runtime.model.requests[1]["messages"][-3])
        events = await self.stream_events()
        self.assertTrue(events[0][1].data["reset"])
        self.assertEqual([item.data["token"] for _, item in events if item.event == "token"], ["年假", "十天"])
        self.assertEqual(events[-1][1].event, "done")
        self.assertNotIn("内部思考", str(events))
        self.assertNotIn("search_query", str(events))
        ttl = await self.redis.ttl(chat_stream_key(self.task.id))
        self.assertTrue(self.settings.terminal_stream_ttl_seconds - 2 <= ttl <= self.settings.terminal_stream_ttl_seconds)

    # 作用：在模型仍未完成时观察首 token，证明不是生成结束后再把答案切片。
    async def test_first_token_precedes_model_completion_and_database_answer(self):
        gate = asyncio.Event()
        self.runtime.model = FakeModel([FakeStream(answer_turn(), pause_at=1, gate=gate)])
        message = FakeMessage(self.task, self.runtime)
        processing = asyncio.create_task(handle_message(message, self.runtime, owner="stream-owner"))
        try:
            for _ in range(100):
                events = await self.stream_events()
                if any(item.event == "token" for _, item in events):
                    break
                await asyncio.sleep(0.02)
            self.assertTrue(any(item.event == "token" for _, item in events))
            task, answer = await self.inspect_task()
            self.assertEqual(task.status, "running")
            self.assertIsNone(answer)
            self.assertFalse(message.processed)
        finally:
            gate.set()
            await processing

    # 作用：验证真实基线脚本经相同受理与续读入口得到与数据库一致的完整回答。
    async def test_baseline_submission_and_event_reader(self):
        from scripts.check_agent_baseline import create_conversation, submit_question, wait_for_answer

        self.conversation_id = await create_conversation(self.sessions, self.user_id)
        request_id = await submit_question(
            self.sessions, user_id=self.user_id, conversation_id=self.conversation_id, question="年假几天？",
        )
        async with self.sessions() as session:
            self.task = await session.get(ChatRequest, request_id)
        await handle_message(FakeMessage(self.task, self.runtime), self.runtime, owner="baseline")
        result = await wait_for_answer(self.sessions, self.runtime.streams, request_id=request_id, timeout_seconds=5)
        self.assertEqual((result["answer"], result["token_event_count"]), ("年假十天", 2))
        self.assertEqual(result["request_id"], request_id)

    # 作用：完成写入的最终租约校验失败时，连同已 flush 的 assistant 和会话序号一起回滚。
    async def test_completion_transaction_rolls_back_on_late_lease_loss(self):
        async with self.sessions() as session, session.begin():
            await claim_task(session, ChatRequest, task_id=self.task.id, owner="owner", lease_seconds=120)
        with patch("infra.mysql.repositories.chat.transition_task", new=AsyncMock(return_value=False)):
            with self.assertRaises(LeaseLost):
                async with self.sessions() as session, session.begin():
                    await finish_chat(session, request_id=self.task.id, owner="owner", attempt=1, answer="不可保存")
        task, answer = await self.inspect_task()
        self.assertEqual(task.status, "running")
        self.assertIsNone(answer)
        async with self.sessions() as session:
            conversation = await session.get(Conversation, self.conversation_id)
            self.assertEqual((conversation.active_request_id, conversation.next_message_sequence), (self.task.id, 2))
            self.assertEqual(len((await session.scalars(select(Message))).all()), 1)

    # 作用：同一任务重复投递只确认已有终态，不重新生成或重复写 assistant 消息。
    async def test_duplicate_delivery_does_not_regenerate(self):
        await handle_message(FakeMessage(self.task, self.runtime), self.runtime, owner="owner-a")
        count = len(self.runtime.model.requests)
        duplicate = FakeMessage(self.task, self.runtime)
        await handle_message(duplicate, self.runtime, owner="owner-b")
        self.assertEqual(duplicate.action, "ack")
        self.assertEqual(len(self.runtime.model.requests), count)
        async with self.sessions() as session:
            answers = (await session.scalars(select(Message).where(Message.request_id == self.task.id, Message.role == "assistant"))).all()
            self.assertEqual(len(answers), 1)

    # 作用：验证半段流断开后持久化重试，下一 attempt 先 reset 并复用成功资料。
    async def test_interrupted_stream_retry_resets_and_restores_snapshot(self):
        self.runtime.model = FakeModel([tool_turn(), [chunk(content="旧半段")], tool_turn("模型重试改变了词"), answer_turn("新答案", "十天")])
        first = FakeMessage(self.task, self.runtime)
        await handle_message(first, self.runtime, owner="owner-a")
        task, answer = await self.inspect_task()
        self.assertEqual((task.status, first.action), ("retry_wait", "ack"))
        self.assertIsNone(answer)
        self.assertFalse(any(item.event == "error" for _, item in await self.stream_events()))
        await self.make_retry_due()
        await handle_message(FakeMessage(self.task, self.runtime), self.runtime, owner="owner-b")
        task, answer = await self.inspect_task()
        self.assertEqual((task.status, task.attempt, answer.content), ("succeeded", 2, "新答案十天"))
        self.assertEqual(self.retrieval.computations, 1)
        self.assertEqual(self.retrieval.calls[0], self.retrieval.calls[1])
        self.assertEqual(self.runtime.model.requests[2]["tool_choice"]["function"]["name"], "RAG")
        self.assertFalse(any(item["role"] == "tool" for item in self.runtime.model.requests[2]["messages"]))
        resets = [item.data["attempt"] for _, item in await self.stream_events() if item.data.get("reset")]
        self.assertEqual(resets, [1, 2])

    # 作用：新提问拥有新的 request_id 和检索上下文，同一会话不会被永久禁用检索。
    async def test_next_question_can_retrieve_again(self):
        await handle_message(FakeMessage(self.task, self.runtime), self.runtime, owner="owner-a")
        self.task = await self.accept("年假要如何申请？")
        self.runtime.model = FakeModel([tool_turn("请假流程 申请 审批"), answer_turn("申请", "审批")])
        await handle_message(FakeMessage(self.task, self.runtime), self.runtime, owner="owner-b")
        self.assertEqual(self.retrieval.computations, 2)
        async with self.sessions() as session:
            runs = (await session.scalars(select(RetrievalRun))).all()
            self.assertEqual(len({run.request_id for run in runs}), 2)

    # 作用：最终失败与 error Outbox 一起落库，释放会话之后才进入死信队列。
    async def test_final_failure_releases_conversation_and_dead_letters(self):
        self.runtime.model = FakeModel([AgentError("model_api_failed", "密钥错误", retryable=False)])
        message = FakeMessage(self.task, self.runtime)
        await handle_message(message, self.runtime, owner="owner-a")
        task, answer = await self.inspect_task()
        self.assertEqual((task.status, message.action, message.status_at_confirmation), ("failed", "reject", "failed"))
        self.assertIsNone(answer)
        async with self.sessions() as session:
            self.assertIsNone((await session.get(Conversation, self.conversation_id)).active_request_id)
        self.assertEqual((await self.stream_events())[-1][1].event, "error")

    # 作用：Redis done 写入失败也不撤销已提交答案，Outbox 恢复后补发且不重新生成。
    async def test_done_outbox_retries_after_redis_failure(self):
        original = self.runtime.streams.emit_terminal
        self.runtime.streams.emit_terminal = AsyncMock(side_effect=AgentError("redis_unavailable", "Redis 不可用"))
        message = FakeMessage(self.task, self.runtime)
        await handle_message(message, self.runtime, owner="owner-a")
        task, answer = await self.inspect_task()
        self.assertEqual((task.status, message.action), ("succeeded", "ack"))
        self.assertEqual(answer.content, "年假十天")
        async with self.sessions() as session, session.begin():
            terminal = await session.scalar(select(OutboxEvent).where(OutboxEvent.destination == "redis_stream"))
            self.assertEqual(terminal.status, "pending")
            terminal.available_at = utc_now() - timedelta(seconds=1)
        self.runtime.streams.emit_terminal = original
        await publish_once(self.runtime, destination="redis_stream")
        self.assertEqual((await self.stream_events())[-1][1].event, "done")
        self.assertEqual(self.retrieval.computations, 1)

    # 作用：活动 Redis 故障安排数据库重试，即使无法回流 error 也保留可查询的事实。
    async def test_active_redis_failure_is_a_durable_retry(self):
        self.runtime.streams.emit_active = AsyncMock(side_effect=AgentError("redis_unavailable", "Redis 不可用"))
        message = FakeMessage(self.task, self.runtime)
        await handle_message(message, self.runtime, owner="owner-a")
        task, answer = await self.inspect_task()
        self.assertEqual((task.status, message.action), ("retry_wait", "ack"))
        self.assertIsNone(answer)
        self.assertEqual(self.runtime.model.requests, [])

    # 作用：无法保存失败安排时 NACK 重投，随后用过期租约恢复重新生成工作通知。
    async def test_uncommitted_failure_is_not_acknowledged(self):
        self.runtime.model = FakeModel([[chunk(content="半段")]])
        message = FakeMessage(self.task, self.runtime)
        with patch("services.agent.delivery.record_failure", new=AsyncMock(side_effect=OSError("数据库暂时不可用"))):
            await handle_message(message, self.runtime, owner="owner-a")
        self.assertEqual(message.action, "requeue")
        async with self.sessions() as session, session.begin():
            task = await session.get(ChatRequest, self.task.id)
            self.assertEqual(task.status, "running")
            task.lease_expires_at = utc_now() - timedelta(seconds=1)
        async with self.sessions() as session, session.begin():
            self.assertEqual(await recover_expired_chats(session, limit=50, max_attempts=3), 1)
        task, answer = await self.inspect_task()
        self.assertEqual(task.status, "retry_wait")
        self.assertIsNone(answer)

    # 作用：停机取消在途生成时先持久化重试，不提交已回流的半段文本。
    async def test_shutdown_cancellation_persists_retry(self):
        gate = asyncio.Event()
        stream = FakeStream(answer_turn(), pause_at=1, gate=gate)
        self.runtime.model = FakeModel([stream])
        message = FakeMessage(self.task, self.runtime)
        processing = asyncio.create_task(handle_message(message, self.runtime, owner="owner-a"))
        for _ in range(100):
            if any(item.event == "token" for _, item in await self.stream_events()):
                break
            await asyncio.sleep(0.02)
        processing.cancel()
        await processing
        task, answer = await self.inspect_task()
        self.assertEqual((task.status, message.action), ("retry_wait", "ack"))
        self.assertIsNone(answer)
        self.assertTrue(stream.closed)

    # 作用：生成超时关闭模型流，持久化重试且不把已输出的半段文本写成最终答案。
    async def test_generation_timeout_closes_stream_and_retries(self):
        gate = asyncio.Event()
        stream = FakeStream(answer_turn(), pause_at=1, gate=gate)
        self.runtime.settings = replace(self.settings, generation_timeout_seconds=0.2)
        self.runtime.model = FakeModel([stream])
        message = FakeMessage(self.task, self.runtime)
        await handle_message(message, self.runtime, owner="slow")
        task, answer = await self.inspect_task()
        self.assertEqual((task.status, task.last_error, message.action), ("retry_wait", "generation_timeout", "ack"))
        self.assertIsNone(answer)
        self.assertTrue(stream.closed)

    # 作用：心跳失去执行权后取消尚在输出的模型，不让旧执行者继续生成或保存答案。
    async def test_heartbeat_failure_cancels_generation(self):
        stream = FakeStream(answer_turn(), pause_at=1, gate=asyncio.Event())
        self.runtime.lease = TaskLeaseSettings(3, 50, 50)
        self.runtime.model = FakeModel([stream])
        message = FakeMessage(self.task, self.runtime)
        with patch("services.agent.processor.renew_task_lease", new=AsyncMock(return_value=False)) as renewal:
            await handle_message(message, self.runtime, owner="old")
        self.assertEqual(renewal.await_count, 1)
        self.assertTrue(stream.closed)
        self.assertIsNone((await self.inspect_task())[1])
        self.assertEqual(message.action, "ack")

    # 作用：模型伪造 user_id 等工具参数只得到工具错误，修正后仍使用可信任务归属检索。
    async def test_tool_identity_cannot_be_forged(self):
        bad = [chunk(calls=[tool_fragment(0, call_id="bad", name="RAG", arguments=json.dumps({"query": "年假", "user_id": new_id()}))], finish="tool_calls")]
        self.runtime.model = FakeModel([bad, tool_turn(), answer_turn()])
        await handle_message(FakeMessage(self.task, self.runtime), self.runtime, owner="owner")
        correction = json.loads(self.runtime.model.requests[1]["messages"][-1]["content"])
        self.assertEqual(correction["code"], "invalid_tool_input")
        self.assertEqual(len(self.retrieval.calls), 1)
        self.assertEqual((await self.inspect_task())[0].status, "succeeded")

    # 作用：重试次数耗尽的过期任务直接进入最终失败，并产生可补发的 error Outbox。
    async def test_recovery_exhaustion_releases_conversation(self):
        async with self.sessions() as session, session.begin():
            await claim_task(session, ChatRequest, task_id=self.task.id, owner="dead", lease_seconds=120)
            task = await session.get(ChatRequest, self.task.id)
            task.attempt = 3
            task.lease_expires_at = utc_now() - timedelta(seconds=1)
        async with self.sessions() as session, session.begin():
            self.assertEqual(await recover_expired_chats(session, limit=50, max_attempts=3), 1)
        async with self.sessions() as session:
            self.assertIsNone((await session.get(Conversation, self.conversation_id)).active_request_id)
        message = FakeMessage(self.task, self.runtime)
        await handle_message(message, self.runtime, owner="later")
        self.assertEqual((message.action, message.status_at_confirmation), ("reject", "failed"))
        self.assertEqual((await self.stream_events())[-1][1].event, "error")
        self.assertEqual(self.runtime.model.requests, [])

    # 作用：过期执行者不能写答案；重领后新 attempt 可正常完成。
    async def test_expired_worker_cannot_overwrite_answer(self):
        async with self.sessions() as session, session.begin():
            self.assertTrue(await claim_task(session, ChatRequest, task_id=self.task.id, owner="old", lease_seconds=120))
        async with self.sessions() as session, session.begin():
            task = await session.get(ChatRequest, self.task.id)
            task.lease_expires_at = utc_now() - timedelta(seconds=1)
        async with self.sessions() as session, session.begin():
            await recover_expired_chats(session, limit=50, max_attempts=3)
        await self.make_retry_due()
        async with self.sessions() as session, session.begin():
            self.assertTrue(await claim_task(session, ChatRequest, task_id=self.task.id, owner="new", lease_seconds=120))
        async with self.sessions() as session, session.begin():
            self.assertFalse(await finish_chat(session, request_id=self.task.id, owner="old", attempt=1, answer="过期答案"))
            self.assertTrue(await finish_chat(session, request_id=self.task.id, owner="new", attempt=2, answer="正确答案"))
        self.assertEqual((await self.inspect_task())[1].content, "正确答案")

    # 作用：验证上下文固定在本次提问，失败轮次之前整体截断，后来的消息不混入。
    async def test_history_cutoff_and_failed_turn_policy(self):
        self.runtime.model = FakeModel([AgentError("bad", "不可恢复", retryable=False)])
        await handle_message(FakeMessage(self.task, self.runtime), self.runtime, owner="old")
        self.task = await self.accept("现在问新问题")
        async with self.sessions() as session, session.begin():
            await claim_task(session, ChatRequest, task_id=self.task.id, owner="new", lease_seconds=120)
            session.add(Message(id=new_id(), conversation_id=self.conversation_id, request_id=new_id(), sequence=999, role="user", content="后来的问题不能混入"))
        async with self.sessions() as session:
            context = await load_chat_context(session, request_id=self.task.id, owner="new", attempt=1)
        self.assertEqual(context.messages, [{"role": "user", "content": "现在问新问题"}])

    # 作用：非法或归属不符的队列通知进入死信，不修改合法任务状态。
    async def test_invalid_notifications_do_not_claim_task(self):
        valid = json.loads(FakeMessage(self.task, self.runtime).body)
        bodies = [b"not json", json.dumps({key: value for key, value in valid.items() if key != "schema_version"}).encode(), json.dumps(valid | {"schema_version": True}).encode(), json.dumps(valid | {"user_id": new_id()}).encode()]
        for body in bodies:
            message = FakeMessage(self.task, self.runtime, body=body)
            await handle_message(message, self.runtime, owner="bad")
            self.assertEqual(message.action, "reject")
        self.assertEqual((await self.inspect_task())[0].status, "pending")

    # 作用：检查 Outbox 只有得到发布确认才标记 sent，拒绝确认时保存退避。
    async def test_publisher_confirm_controls_outbox_sent_state(self):
        self.runtime.exchange.confirmed = False
        await publish_once(self.runtime, destination="rabbitmq")
        async with self.sessions() as session, session.begin():
            row = await session.scalar(select(OutboxEvent))
            self.assertEqual(row.status, "pending")
            row.available_at = utc_now() - timedelta(seconds=1)
        self.runtime.exchange.confirmed = True
        await publish_once(self.runtime, destination="rabbitmq")
        async with self.sessions() as session:
            self.assertEqual((await session.scalar(select(OutboxEvent))).status, "sent")
        published, options = self.runtime.exchange.published[-1]
        self.assertTrue(options["mandatory"])
        self.assertEqual(int(published.delivery_mode), 2)

    # 作用：验证 Lua 拒绝旧 attempt，终态重复补发不新增事件，活动心跳不能延长终态保留期。
    async def test_lua_attempt_fencing_terminal_dedup_and_ttl(self):
        streams = self.runtime.streams
        for attempt, owner in [(1, "old"), (2, "new")]:
            await streams.emit_active(ChatStreamEvent(event="status", data={"request_id": self.task.id, "attempt": attempt, "phase": "started", "reset": True}), owner=owner)
        with self.assertRaises(LeaseLost):
            await streams.emit_active(ChatStreamEvent(event="token", data={"request_id": self.task.id, "attempt": 1, "token": "旧文本"}), owner="old")
        done = ChatStreamEvent(event="done", data={"request_id": self.task.id, "attempt": 2, "event_id": new_id()})
        self.assertEqual(await streams.emit_terminal(done), await streams.emit_terminal(done))
        self.assertEqual(len(await self.stream_events()), 3)
        with self.assertRaises(LeaseLost):
            await streams.touch_active(request_id=self.task.id, attempt=2, owner="new")
        self.assertLessEqual(await self.redis.ttl(chat_stream_key(self.task.id)), self.settings.terminal_stream_ttl_seconds)


class AgentHttpTests(unittest.IsolatedAsyncioTestCase):
    # 作用：关闭超时或失败时仍释放其余客户端，避免停机无限等待某一个连接。
    async def test_resource_cleanup_survives_close_failure(self):
        connection = SimpleNamespace(close=AsyncMock(side_effect=OSError("断开的通道")))
        model = SimpleNamespace(close=AsyncMock())
        http, redis = SimpleNamespace(aclose=AsyncMock()), SimpleNamespace(aclose=AsyncMock())
        engine = SimpleNamespace(dispose=AsyncMock())
        runtime = AgentRuntime(AgentSettings(), None, None, engine, model, None, None, http, redis, connection)
        await runtime.close()
        for close in (connection.close, model.close, http.aclose, redis.aclose, engine.dispose):
            self.assertEqual(close.await_count, 1)

    # 作用：验证业务信号量只启动两个任务，停机停止领取并取消超过宽限的在途任务。
    async def test_consumer_concurrency_and_shutdown(self):
        messages = deque([object() for _ in range(4)])
        gate, stop, active = asyncio.Event(), asyncio.Event(), set()
        entered, cancelled = [], []

        class Iterator:
            # 作用：进入队列迭代上下文，不引入真正的 RabbitMQ 网络依赖。
            async def __aenter__(self):
                return self

            # 作用：关闭测试迭代上下文，实际未消费的通知保持在队列中。
            async def __aexit__(self, *args):
                return False

            # 作用：只在消费者确实调用领取时取出下一个通知。
            async def __anext__(self):
                return messages.popleft()

        # 作用：阻塞业务处理，观察实际进入数并记录宽限超时触发的取消。
        async def process(message, runtime, *, owner):
            entered.append(message)
            try:
                await gate.wait()
            except asyncio.CancelledError:
                cancelled.append(message)
                raise

        runtime = SimpleNamespace(settings=AgentSettings(concurrency=2), queue=SimpleNamespace(iterator=Iterator))
        with patch("services.agent.worker.handle_message", new=process):
            consumer = asyncio.create_task(consume_messages(runtime, active=active, stop=stop))
            try:
                for _ in range(100):
                    if len(entered) == 2:
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual((len(entered), len(messages)), (2, 2))
            finally:
                stop.set()
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
                await drain_tasks(active, grace_seconds=0)
            self.assertEqual(len(cancelled), 2)

    # 作用：验证 HTTP 适配器传递绑定任务的服务 JWT，并区分暂时故障与输入冲突。
    async def test_retrieval_http_identity_and_error_policy(self):
        credentials = ServiceTokenSettings("s" * 48)
        user_id, request_id = new_id(), new_id()
        payload = RetrieveRequest(request_id=request_id, messages=[{"role": "user", "content": "年假？"}], search_query="年假")
        status = 200

        # 作用：检查模型工具无法伪造用户参数，响应可按测试切换为暂时或永久错误。
        async def handle(request):
            principal = verify_retrieval_token(credentials, request.headers["Authorization"][7:])
            self.assertEqual((principal.user_id, principal.request_id), (user_id, request_id))
            body = json.loads(request.content)
            self.assertNotIn("user_id", body)
            if status == 200:
                return httpx.Response(200, json=build_retrieve_response(request_id, []).model_dump(mode="json"))
            return httpx.Response(status, json={"detail": {"code": "retrieval_in_progress" if status == 409 else "history_mismatch"}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            client = RetrievalClient(http, settings=AgentSettings(retrieval_url="https://retrieval.test"), credentials=credentials)
            self.assertEqual((await client.retrieve(user_id, payload)).status, "empty")
            status = 409
            with self.assertRaises(AgentError) as raised:
                await client.retrieve(user_id, payload)
            self.assertTrue(raised.exception.retryable)
            status = 422
            with self.assertRaises(AgentError) as raised:
                await client.retrieve(user_id, payload)
            self.assertFalse(raised.exception.retryable)

    # 作用：验证 Agent 配置不依赖 Qdrant 或 Retrieval 自身的模型运行参数。
    async def test_component_scoped_configuration(self):
        with patch("core.config._dotenv_values", return_value={}), patch.dict("os.environ", {"RETRIEVAL_SERVICE_SECRET": "s" * 48, "RETRIEVAL_QUERY_MODE": "invalid-for-retrieval"}, clear=True):
            self.assertEqual(load_agent_settings().concurrency, 2)
            self.assertEqual(load_retrieval_token_settings().service_secret, "s" * 48)


if __name__ == "__main__":
    unittest.main()
