"""瞬态无侵入消息采样与自适应解码集成测试 (最高测试接缝).

@author Ateng
@since 2026-10-04
"""

import base64
import json
from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition

from mcp_server_kafka.config import KafkaConfig
from mcp_server_kafka.manager import KafkaManager
from mcp_server_kafka.models import PartitionInfo, TopicDetail
from mcp_server_kafka.server import create_mcp_server


class FakeConsumerRecord:
    """模拟 Kafka 消费记录项.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(
        self,
        partition: int,
        offset: int,
        value: bytes | None,
        key: bytes | None = None,
        timestamp: int = 1728045600000,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self.partition = partition
        self.offset = offset
        self.value = value
        self.key = key
        self.timestamp = timestamp
        self.headers = headers or []


class FakeAIOKafkaConsumer:
    """用于测试接缝注入的底层异步 Consumer 桩.

    严格监测并记录是否向集群提交位移.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self, enable_auto_commit: bool, group_id: str | None) -> None:
        self.enable_auto_commit = enable_auto_commit
        self.group_id = group_id
        self.commit_called: bool = False
        self.assigned_partitions: list[TopicPartition] = []
        self.records_to_return: list[FakeConsumerRecord] = []
        self.seek_calls: list[tuple[Any, ...]] = []

    async def start(self) -> None:
        """模拟启动消费者."""

    async def stop(self) -> None:
        """模拟关闭消费者."""

    def assign(self, partitions: list[TopicPartition]) -> None:
        """记录分配的分区列表."""
        self.assigned_partitions = partitions

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict[TopicPartition, int]:
        """模拟返回各分区的末尾位移."""
        return {tp: 1000 for tp in partitions}

    async def seek_to_beginning(self, *partitions: TopicPartition) -> None:
        """记录 seek_to_beginning 调用."""
        self.seek_calls.append(("seek_to_beginning", partitions))

    def seek(self, partition: TopicPartition, offset: int) -> None:
        """记录指定位移 seek 调用."""
        self.seek_calls.append(("seek", partition, offset))

    async def getmany(
        self,
        *partitions: TopicPartition,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[FakeConsumerRecord]]:
        """模拟批量拉取消息."""
        if not self.assigned_partitions:
            return {}
        tp = self.assigned_partitions[0]
        limit = max_records or len(self.records_to_return)
        return {tp: self.records_to_return[:limit]}

    async def commit(self, *args: Any, **kwargs: Any) -> None:
        """监测是否发生非法的位移提交动作."""
        self.commit_called = True


class StubbedConsumerKafkaManager(KafkaManager):
    """注入底层 FakeAIOKafkaConsumer 的连接管理器.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self, config: KafkaConfig | None = None) -> None:
        super().__init__(config or KafkaConfig(bootstrap_servers="mock:9092"))
        self.fake_consumer = FakeAIOKafkaConsumer(enable_auto_commit=False, group_id=None)

    def create_consumer(self) -> AIOKafkaConsumer:
        """返回注入并处于受控监测的 fake consumer 桩."""
        return self.fake_consumer  # type: ignore[return-value]

    async def describe_topic(self, topic_name: str) -> TopicDetail:
        """返回预设的主题分区明细."""
        return TopicDetail(
            name=topic_name,
            partitions=[PartitionInfo(partition_id=0, leader=1, replicas=[1], isr=[1])],
        )


@pytest.mark.asyncio
async def test_tool_sample_json_message() -> None:
    """验证采样读取并自适应反序列化 JSON 结构化数据，且绝不提交位移."""
    manager = StubbedConsumerKafkaManager()
    manager.fake_consumer.records_to_return = [
        FakeConsumerRecord(
            partition=0,
            offset=100,
            key=b"key-1",
            value=json.dumps({"order_id": "OD100", "price": 88.5}).encode(),
            headers=[("content-type", b"application/json")],
        )
    ]
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 1. 协议层调用采样
    result = await server.call_tool("kafka_sample_messages", {"topic": "orders", "limit": 5})
    assert result.is_error is False

    # 2. 验证响应与自适应 JSON 解码
    data: dict[str, Any] = json.loads(result.content[0].text)
    assert data["topic"] == "orders"
    assert data["total"] == 1
    msg = data["messages"][0]
    assert msg["encoding"] == "json"
    assert msg["value"]["order_id"] == "OD100"
    assert msg["value"]["price"] == 88.5
    assert msg["key"] == "key-1"
    assert msg["headers"] == {"content-type": "application/json"}

    # 3. 验证零位移影响约束 (Zero Commit Guarantee)
    assert manager.fake_consumer.enable_auto_commit is False
    assert manager.fake_consumer.group_id is None
    assert manager.fake_consumer.commit_called is False


@pytest.mark.asyncio
async def test_tool_sample_plain_text_message() -> None:
    """验证采样读取纯文本字符串数据并标注 text 编码."""
    manager = StubbedConsumerKafkaManager()
    text_content = "这是一条纯文本事件日志"
    manager.fake_consumer.records_to_return = [
        FakeConsumerRecord(
            partition=0,
            offset=201,
            key=None,
            value=text_content.encode(),
        )
    ]
    server = create_mcp_server(KafkaConfig(), manager=manager)

    result = await server.call_tool(
        "kafka_sample_messages",
        {"topic": "logs", "strategy": "earliest"},
    )
    assert result.is_error is False

    data: dict[str, Any] = json.loads(result.content[0].text)
    assert data["total"] == 1
    msg = data["messages"][0]
    assert msg["encoding"] == "text"
    assert msg["value"] == text_content
    assert msg["size"] == len(text_content.encode())

    # 验证零提交位移
    assert manager.fake_consumer.commit_called is False


@pytest.mark.asyncio
async def test_tool_sample_binary_message_with_base64_fallback() -> None:
    """验证遇到非 UTF-8 二进制字节流时安全降级为 Base64 编码."""
    manager = StubbedConsumerKafkaManager()
    raw_binary = b"\x00\x01\x80\xff\xfe\x00\xaa"
    manager.fake_consumer.records_to_return = [
        FakeConsumerRecord(
            partition=0,
            offset=305,
            key=b"\xbb\xcc",
            value=raw_binary,
        )
    ]
    server = create_mcp_server(KafkaConfig(), manager=manager)

    result = await server.call_tool(
        "kafka_sample_messages",
        {"topic": "bin-data", "strategy": "offset", "offset": 300},
    )
    assert result.is_error is False

    data: dict[str, Any] = json.loads(result.content[0].text)
    msg = data["messages"][0]
    assert msg["encoding"] == "base64"
    assert msg["value"] == base64.b64encode(raw_binary).decode("ascii")


@pytest.mark.asyncio
async def test_tool_sample_limit_constraint() -> None:
    """验证采样读取的 limit 参数强约束为 100 限制."""
    manager = StubbedConsumerKafkaManager()
    manager.fake_consumer.records_to_return = [
        FakeConsumerRecord(partition=0, offset=i, value=b"test") for i in range(120)
    ]
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 1. 传入超出 100 的 limit
    result = await server.call_tool(
        "kafka_sample_messages",
        {"topic": "bulk-topic", "limit": 200},
    )
    assert result.is_error is False

    # 2. 验证实际返回截断在 100 条
    data: dict[str, Any] = json.loads(result.content[0].text)
    assert data["total"] == 100
    assert len(data["messages"]) == 100
