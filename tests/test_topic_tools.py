"""主题管理与安全防线机制集成测试 (最高测试接缝).

@author Ateng
@since 2026-10-04
"""

import json
from typing import Any

import pytest

from mcp_server_kafka.config import KafkaConfig
from mcp_server_kafka.manager import KafkaManager
from mcp_server_kafka.models import PartitionInfo, TopicDetail, TopicSummary
from mcp_server_kafka.server import create_mcp_server


class FakeTopicKafkaManager(KafkaManager):
    """用于测试接缝注入的主题管理桩.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self, config: KafkaConfig | None = None) -> None:
        super().__init__(config or KafkaConfig(bootstrap_servers="mock:9092"))
        self.topics: dict[str, TopicDetail] = {
            "order-created": TopicDetail(
                name="order-created",
                is_internal=False,
                partitions=[
                    PartitionInfo(partition_id=0, leader=1, replicas=[1, 2], isr=[1, 2]),
                    PartitionInfo(partition_id=1, leader=2, replicas=[1, 2], isr=[1, 2]),
                ],
                configs={"retention.ms": "604800000"},
            ),
            "user-events": TopicDetail(
                name="user-events",
                is_internal=False,
                partitions=[
                    PartitionInfo(partition_id=0, leader=1, replicas=[1], isr=[1]),
                ],
            ),
            "__consumer_offsets": TopicDetail(
                name="__consumer_offsets",
                is_internal=True,
                partitions=[
                    PartitionInfo(partition_id=0, leader=1, replicas=[1], isr=[1]),
                ],
            ),
        }

    async def list_topics(
        self,
        pattern: str | None = None,
        include_internal: bool = False,
    ) -> list[TopicSummary]:
        """按过滤条件列出所有主题概要信息."""
        results: list[TopicSummary] = []
        for name, detail in self.topics.items():
            if not include_internal and detail.is_internal:
                continue
            if pattern and pattern not in name:
                continue
            results.append(
                TopicSummary(
                    name=name,
                    partitions_count=len(detail.partitions),
                    is_internal=detail.is_internal,
                )
            )
        return results

    async def describe_topic(self, topic_name: str) -> TopicDetail:
        """获取指定主题详细信息."""
        if topic_name not in self.topics:
            raise ValueError(f"主题 '{topic_name}' 不存在")
        return self.topics[topic_name]

    async def create_topic(
        self,
        topic_name: str,
        partitions: int = 1,
        replication_factor: int = 1,
    ) -> dict[str, Any]:
        """模拟创建新主题."""
        if topic_name in self.topics:
            raise ValueError(f"主题 '{topic_name}' 已存在")
        self.topics[topic_name] = TopicDetail(
            name=topic_name,
            is_internal=False,
            partitions=[
                PartitionInfo(partition_id=idx, leader=1, replicas=[1], isr=[1])
                for idx in range(partitions)
            ],
        )
        return {"topic": topic_name, "partitions": partitions, "status": "created"}

    async def delete_topic(self, topic_name: str) -> dict[str, Any]:
        """模拟删除指定主题."""
        if topic_name not in self.topics:
            raise ValueError(f"主题 '{topic_name}' 不存在")
        del self.topics[topic_name]
        return {"topic": topic_name, "status": "deleted"}


@pytest.mark.asyncio
async def test_tool_kafka_list_topics() -> None:
    """验证通过 kafka_list_topics 工具列出主题及模式过滤与内部主题隔离."""
    manager = FakeTopicKafkaManager()
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 1. 默认查询不包含内部主题
    result = await server.call_tool("kafka_list_topics", {})
    assert result.is_error is False
    data: dict[str, Any] = json.loads(result.content[0].text)
    topic_names = [item["name"] for item in data["topics"]]
    assert "order-created" in topic_names
    assert "user-events" in topic_names
    assert "__consumer_offsets" not in topic_names
    assert data["count"] == 2

    # 2. 指定 pattern 过滤
    filtered = await server.call_tool("kafka_list_topics", {"pattern": "order"})
    filtered_data: dict[str, Any] = json.loads(filtered.content[0].text)
    assert filtered_data["count"] == 1
    assert filtered_data["topics"][0]["name"] == "order-created"


@pytest.mark.asyncio
async def test_tool_kafka_describe_topic_and_resource() -> None:
    """验证获取主题详情与动态 Resource 上下文读取契约."""
    manager = FakeTopicKafkaManager()
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 1. 调用 kafka_describe_topic 查询主题
    result = await server.call_tool("kafka_describe_topic", {"topic_name": "order-created"})
    assert result.is_error is False
    detail: dict[str, Any] = json.loads(result.content[0].text)
    assert detail["name"] == "order-created"
    assert len(detail["partitions"]) == 2
    assert detail["configs"].get("retention.ms") == "604800000"

    # 2. 读取动态上下文 Resource kafka://topics/order-created
    resources = await server.read_resource("kafka://topics/order-created")
    assert len(resources) >= 1
    content = resources[0].content
    assert "order-created" in content
    assert "分区 0" in content


@pytest.mark.asyncio
async def test_tool_kafka_create_and_delete_topic_safety() -> None:
    """验证主题创建与删除操作，重点验证 confirm=True 二次确认防线."""
    manager = FakeTopicKafkaManager()
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 1. 创建新主题
    create_res = await server.call_tool(
        "kafka_create_topic",
        {"topic_name": "new-test-topic", "partitions": 3, "replication_factor": 1},
    )
    assert create_res.is_error is False
    assert "new-test-topic" in create_res.content[0].text

    # 2. 未显式传 confirm=True 时拦截删除
    del_reject = await server.call_tool(
        "kafka_delete_topic",
        {"topic_name": "new-test-topic", "confirm": False},
    )
    assert "confirm=True" in del_reject.content[0].text or del_reject.is_error is True
    assert "new-test-topic" in manager.topics

    # 3. 传入 confirm=True 确认删除成功
    del_success = await server.call_tool(
        "kafka_delete_topic",
        {"topic_name": "new-test-topic", "confirm": True},
    )
    assert del_success.is_error is False
    assert "new-test-topic" not in manager.topics


@pytest.mark.asyncio
async def test_read_only_mode_defense() -> None:
    """验证在全局只读模式下隐藏写工具并拦截破坏性操作."""
    read_only_config = KafkaConfig(read_only=True)
    manager = FakeTopicKafkaManager(read_only_config)
    server = create_mcp_server(read_only_config, manager=manager)

    # 验证工具清单中未暴露写操作工具
    tools_result = await server.list_tools()
    tool_names = [tool.name for tool in tools_result]
    assert "kafka_list_topics" in tool_names
    assert "kafka_describe_topic" in tool_names
    assert "kafka_create_topic" not in tool_names
    assert "kafka_delete_topic" not in tool_names
