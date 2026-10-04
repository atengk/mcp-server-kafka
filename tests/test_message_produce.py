"""消息生产通道与多载荷支持集成测试 (最高测试接缝).

@author Ateng
@since 2026-10-04
"""

import json
from typing import Any

import pytest
from aiokafka import AIOKafkaProducer

from mcp_server_kafka.config import KafkaConfig
from mcp_server_kafka.manager import KafkaManager
from mcp_server_kafka.server import create_mcp_server


class FakeRecordMetadata:
    """模拟 Kafka 消息写入确认元数据.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self, topic: str, partition: int, offset: int, timestamp: int) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp


class FakeAIOKafkaProducer:
    """用于测试接缝注入的底层异步 Producer 桩.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self) -> None:
        self.sent_calls: list[dict[str, Any]] = []

    async def start(self) -> None:
        """模拟启动客户端."""

    async def stop(self) -> None:
        """模拟关闭客户端."""

    async def send_and_wait(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> FakeRecordMetadata:
        """记录真实编码后的字节参数并返回写入确认元数据."""
        self.sent_calls.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "partition": partition,
                "headers": headers,
            }
        )
        return FakeRecordMetadata(
            topic=topic,
            partition=partition or 0,
            offset=len(self.sent_calls) + 100,
            timestamp=1728045600000,
        )


class StubbedKafkaManager(KafkaManager):
    """注入底层 FakeAIOKafkaProducer 的连接管理器.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self, config: KafkaConfig | None = None) -> None:
        super().__init__(config or KafkaConfig(bootstrap_servers="mock:9092"))
        self.fake_producer = FakeAIOKafkaProducer()

    async def get_producer(self) -> AIOKafkaProducer:
        """返回测试注入的 fake producer 桩."""
        return self.fake_producer  # type: ignore[return-value]


@pytest.mark.asyncio
async def test_tool_produce_plain_text_message() -> None:
    """验证通过 kafka_produce_message 工具生产纯文本消息并编码为字节流."""
    manager = StubbedKafkaManager()
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 1. 协议层调用发送纯文本消息
    result = await server.call_tool(
        "kafka_produce_message",
        {"topic": "text-topic", "value": "hello kafka agent"},
    )
    assert result.is_error is False

    # 2. 校验返回契约与真实字节流转换
    data: dict[str, Any] = json.loads(result.content[0].text)
    assert data["topic"] == "text-topic"
    assert data["partition"] == 0
    assert data["offset"] >= 100

    sent_calls = manager.fake_producer.sent_calls
    assert len(sent_calls) == 1
    assert sent_calls[0]["value"] == b"hello kafka agent"


@pytest.mark.asyncio
async def test_tool_produce_json_payload_with_key_and_headers() -> None:
    """验证发送 JSON 字典载荷自动序列化，并正确编码消息键与自定义标头."""
    manager = StubbedKafkaManager()
    server = create_mcp_server(KafkaConfig(), manager=manager)

    payload = {"order_id": 999, "amount": 199.9, "status": "PAID"}
    result = await server.call_tool(
        "kafka_produce_message",
        {
            "topic": "orders",
            "value": payload,
            "key": "order-999",
            "partition": 2,
            "headers": {"trace-id": "tr-abc-123"},
        },
    )
    assert result.is_error is False

    # 1. 验证 MCP 响应
    data: dict[str, Any] = json.loads(result.content[0].text)
    assert data["topic"] == "orders"
    assert data["partition"] == 2
    assert data["key"] == "order-999"

    # 2. 验证底层真实编码：JSON UTF-8 字节流、Key 字节流及 Headers 格式
    sent = manager.fake_producer.sent_calls[0]
    assert sent["key"] == b"order-999"
    assert sent["partition"] == 2
    assert sent["headers"] == [("trace-id", b"tr-abc-123")]
    assert json.loads(sent["value"].decode("utf-8")) == payload


@pytest.mark.asyncio
async def test_produce_message_read_only_defense() -> None:
    """验证在全局只读模式下隐藏 kafka_produce_message 工具."""
    read_only_config = KafkaConfig(read_only=True)
    manager = StubbedKafkaManager(read_only_config)
    server = create_mcp_server(read_only_config, manager=manager)

    tools_result = await server.list_tools()
    tool_names = [tool.name for tool in tools_result]
    assert "kafka_produce_message" not in tool_names
