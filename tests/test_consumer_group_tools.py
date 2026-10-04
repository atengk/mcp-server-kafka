"""消费组监测与积压 (Lag) 诊断接口集成测试 (最高测试接缝).

@author Ateng
@since 2026-10-04
"""

import json
from typing import Any

import pytest
from aiokafka.coordinator.protocol import ConsumerProtocolMemberAssignment
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from mcp_server_kafka.config import KafkaConfig
from mcp_server_kafka.manager import KafkaManager
from mcp_server_kafka.models import PartitionInfo, TopicDetail
from mcp_server_kafka.server import create_mcp_server


class FakeAdminForGroups:
    """用于消费组测试接缝注入的 Fake Admin 客户端.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(
        self,
        groups_list: list[tuple[str, str]] | None = None,
        described_groups: list[dict[str, Any]] | None = None,
        group_offsets: dict[str, dict[TopicPartition, OffsetAndMetadata]] | None = None,
    ) -> None:
        self.groups_list = groups_list or []
        self.described_groups = described_groups or []
        self.group_offsets = group_offsets or {}

    async def list_consumer_groups(self) -> list[tuple[str, str]]:
        return self.groups_list

    async def describe_consumer_groups(self, group_ids: list[str]) -> list[Any]:
        # 构造类似 DescribeGroupsResponse 结构
        matched = []
        for g_dict in self.described_groups:
            if g_dict.get("group") in group_ids:
                matched.append(
                    (
                        g_dict.get("error_code", 0),
                        g_dict.get("group", ""),
                        g_dict.get("state", "Unknown"),
                        g_dict.get("protocol_type", "consumer"),
                        g_dict.get("protocol", "range"),
                        g_dict.get("members", []),
                    )
                )

        class FakeDescResponse:
            def __init__(self, groups: list[Any]) -> None:
                self.groups = groups

        return [FakeDescResponse(matched)]

    async def list_consumer_group_offsets(
        self, group_id: str
    ) -> dict[TopicPartition, OffsetAndMetadata]:
        return self.group_offsets.get(group_id, {})


class FakeConsumerForOffsets:
    """用于消费组 LEO 查询打桩的异步 Consumer.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self, end_offsets_map: dict[TopicPartition, int] | None = None) -> None:
        self.end_offsets_map = end_offsets_map or {}
        self.queried_partitions: list[TopicPartition] = []

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict[TopicPartition, int]:
        self.queried_partitions.extend(partitions)
        return {tp: self.end_offsets_map.get(tp, 0) for tp in partitions}


class GroupTestKafkaManager(KafkaManager):
    """注入消费组与位移测试桩的 Kafka 管理器.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(
        self,
        fake_admin: FakeAdminForGroups,
        fake_consumer: FakeConsumerForOffsets,
        known_topics: list[TopicDetail] | None = None,
    ) -> None:
        super().__init__(KafkaConfig(bootstrap_servers="mock:9092"))
        self._fake_admin = fake_admin
        self._fake_consumer = fake_consumer
        self._known_topics = known_topics or []

    async def get_admin_client(self) -> Any:
        return self._fake_admin

    def create_consumer(self) -> Any:
        return self._fake_consumer

    async def list_topics(self, pattern: str | None = None, include_internal: bool = False) -> Any:
        from mcp_server_kafka.models import TopicSummary

        return [
            TopicSummary(
                name=t.name,
                partitions_count=len(t.partitions),
                is_internal=t.is_internal,
            )
            for t in self._known_topics
        ]

    async def describe_topic(self, topic_name: str) -> TopicDetail:
        for t in self._known_topics:
            if t.name == topic_name:
                return t
        raise ValueError(f"主题 '{topic_name}' 不存在")


@pytest.mark.asyncio
async def test_tool_kafka_list_consumer_groups_success() -> None:
    """验证在最高测试接缝下调用 kafka_list_consumer_groups 列出所有消费组及状态."""
    fake_admin = FakeAdminForGroups(
        groups_list=[("group-alpha", "consumer"), ("group-beta", "consumer")],
        described_groups=[
            {"group": "group-alpha", "state": "Stable", "protocol_type": "consumer"},
            {"group": "group-beta", "state": "Empty", "protocol_type": "consumer"},
        ],
    )
    fake_consumer = FakeConsumerForOffsets()
    manager = GroupTestKafkaManager(fake_admin, fake_consumer)
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 1. 顶层协议调用 Tool
    result = await server.call_tool("kafka_list_consumer_groups", {})

    # 2. 校验返回结果契约
    assert result.is_error is False
    data = json.loads(result.content[0].text)
    assert data["count"] == 2
    groups = data["groups"]
    assert groups[0]["group_id"] == "group-alpha"
    assert groups[0]["state"] == "Stable"
    assert groups[1]["group_id"] == "group-beta"
    assert groups[1]["state"] == "Empty"


@pytest.mark.asyncio
async def test_tool_kafka_describe_consumer_group_normal_lag() -> None:
    """验证正常消费组各分区 Committed Offset、LEO、Lag 以及成员分配的准确计算."""
    # 构造 assignment 字节
    assign_bytes = ConsumerProtocolMemberAssignment(
        version=0,
        assignment=[("orders", [0, 1])],
        user_data=b"",
    ).encode()

    fake_admin = FakeAdminForGroups(
        described_groups=[
            {
                "group": "group-order-consumer",
                "state": "Stable",
                "protocol_type": "consumer",
                "protocol": "range",
                "members": [
                    (
                        "member-1",
                        "client-app-1",
                        "/192.168.1.100",
                        b"",
                        assign_bytes,
                    )
                ],
            }
        ],
        group_offsets={
            "group-order-consumer": {
                TopicPartition("orders", 0): OffsetAndMetadata(100, ""),
                TopicPartition("orders", 1): OffsetAndMetadata(150, ""),
            }
        },
    )

    fake_consumer = FakeConsumerForOffsets(
        end_offsets_map={
            TopicPartition("orders", 0): 120,
            TopicPartition("orders", 1): 200,
        }
    )

    known_topics = [
        TopicDetail(
            name="orders",
            is_internal=False,
            partitions=[
                PartitionInfo(partition_id=0, leader=1, replicas=[1], isr=[1]),
                PartitionInfo(partition_id=1, leader=2, replicas=[2], isr=[2]),
            ],
            configs={},
        )
    ]

    manager = GroupTestKafkaManager(fake_admin, fake_consumer, known_topics=known_topics)
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 1. 顶层协议调用 Tool
    result = await server.call_tool(
        "kafka_describe_consumer_group",
        {"group_id": "group-order-consumer"},
    )

    # 2. 验证计算契约与 Lag
    assert result.is_error is False
    data = json.loads(result.content[0].text)

    assert data["group_id"] == "group-order-consumer"
    assert data["state"] == "Stable"
    assert len(data["members"]) == 1
    member = data["members"][0]
    assert member["member_id"] == "member-1"
    assert member["client_id"] == "client-app-1"
    assert member["client_host"] == "/192.168.1.100"
    assert len(member["partitions"]) == 2

    # 分区 0: LEO 120 - Committed 100 = Lag 20
    # 分区 1: LEO 200 - Committed 150 = Lag 50
    # Total Lag: 70
    assert data["total_lag"] == 70
    assert len(data["partitions"]) == 2
    p0 = data["partitions"][0]
    assert p0["topic"] == "orders"
    assert p0["partition"] == 0
    assert p0["committed_offset"] == 100
    assert p0["log_end_offset"] == 120
    assert p0["lag"] == 20
    assert p0["member_id"] == "member-1"
    assert p0["topic_deleted"] is False

    p1 = data["partitions"][1]
    assert p1["lag"] == 50


@pytest.mark.asyncio
async def test_tool_kafka_describe_consumer_group_new_partition_without_offset() -> None:
    """边界验证：物理分区扩容但消费组尚未提交位移时，Lag 应等于 LEO 且无异常."""
    assign_bytes = ConsumerProtocolMemberAssignment(
        version=0,
        assignment=[("orders", [0, 1])],
        user_data=b"",
    ).encode()

    fake_admin = FakeAdminForGroups(
        described_groups=[
            {
                "group": "group-new-part",
                "state": "Stable",
                "protocol_type": "consumer",
                "protocol": "range",
                "members": [("member-1", "client-1", "/127.0.0.1", b"", assign_bytes)],
            }
        ],
        group_offsets={
            "group-new-part": {
                TopicPartition("orders", 0): OffsetAndMetadata(50, ""),
                # 分区 1 尚未提交位移
            }
        },
    )

    fake_consumer = FakeConsumerForOffsets(
        end_offsets_map={
            TopicPartition("orders", 0): 60,
            TopicPartition("orders", 1): 30,
        }
    )

    known_topics = [
        TopicDetail(
            name="orders",
            is_internal=False,
            partitions=[
                PartitionInfo(partition_id=0, leader=1, replicas=[1], isr=[1]),
                PartitionInfo(partition_id=1, leader=1, replicas=[1], isr=[1]),
            ],
            configs={},
        )
    ]

    manager = GroupTestKafkaManager(fake_admin, fake_consumer, known_topics=known_topics)
    server = create_mcp_server(KafkaConfig(), manager=manager)

    result = await server.call_tool(
        "kafka_describe_consumer_group",
        {"group_id": "group-new-part"},
    )
    assert result.is_error is False
    data = json.loads(result.content[0].text)

    # 分区 0: 60 - 50 = 10; 分区 1: 未提交，lag = end_offset (30); total_lag = 40
    assert data["total_lag"] == 40
    p1 = next(p for p in data["partitions"] if p["partition"] == 1)
    assert p1["committed_offset"] is None
    assert p1["log_end_offset"] == 30
    assert p1["lag"] == 30


@pytest.mark.asyncio
async def test_tool_kafka_describe_consumer_group_deleted_topic_boundary() -> None:
    """边界验证：妥善处理位移表中已删除的 Topic，避免向 Consumer 请求导致挂起."""
    fake_admin = FakeAdminForGroups(
        described_groups=[
            {
                "group": "group-with-deleted-topic",
                "state": "Empty",
                "protocol_type": "consumer",
                "protocol": "",
                "members": [],
            }
        ],
        group_offsets={
            "group-with-deleted-topic": {
                TopicPartition("deleted-legacy-topic", 0): OffsetAndMetadata(999, ""),
            }
        },
    )

    fake_consumer = FakeConsumerForOffsets(end_offsets_map={})
    # 当前集群中无 deleted-legacy-topic
    manager = GroupTestKafkaManager(fake_admin, fake_consumer, known_topics=[])
    server = create_mcp_server(KafkaConfig(), manager=manager)

    result = await server.call_tool(
        "kafka_describe_consumer_group",
        {"group_id": "group-with-deleted-topic"},
    )
    assert result.is_error is False
    data = json.loads(result.content[0].text)

    # 验证未向 fake_consumer 发起对已删除 topic 的 end_offsets 查询
    assert len(fake_consumer.queried_partitions) == 0

    assert len(data["partitions"]) == 1
    p = data["partitions"][0]
    assert p["topic"] == "deleted-legacy-topic"
    assert p["topic_deleted"] is True
    assert p["log_end_offset"] is None
    assert p["lag"] is None
    assert data["total_lag"] == 0


@pytest.mark.asyncio
async def test_tool_kafka_describe_consumer_group_empty_group() -> None:
    """边界验证：无成员且未提交位移的空消费组正确返回."""
    fake_admin = FakeAdminForGroups(
        described_groups=[
            {
                "group": "group-idle-empty",
                "state": "Empty",
                "protocol_type": "consumer",
                "protocol": "",
                "members": [],
            }
        ],
        group_offsets={},
    )
    fake_consumer = FakeConsumerForOffsets()
    manager = GroupTestKafkaManager(fake_admin, fake_consumer, known_topics=[])
    server = create_mcp_server(KafkaConfig(), manager=manager)

    result = await server.call_tool(
        "kafka_describe_consumer_group",
        {"group_id": "group-idle-empty"},
    )
    assert result.is_error is False
    data = json.loads(result.content[0].text)
    assert data["group_id"] == "group-idle-empty"
    assert data["state"] == "Empty"
    assert data["members"] == []
    assert data["partitions"] == []
    assert data["total_lag"] == 0


@pytest.mark.asyncio
async def test_tool_kafka_describe_consumer_group_not_found() -> None:
    """验证当目标消费组不存在时返回错误描述信息."""
    fake_admin = FakeAdminForGroups(described_groups=[])
    fake_consumer = FakeConsumerForOffsets()
    manager = GroupTestKafkaManager(fake_admin, fake_consumer)
    server = create_mcp_server(KafkaConfig(), manager=manager)

    result = await server.call_tool(
        "kafka_describe_consumer_group",
        {"group_id": "non-existent-group"},
    )
    assert result.is_error is False
    data = json.loads(result.content[0].text)
    assert "error" in data
    assert "不存在" in data["error"]


@pytest.mark.asyncio
async def test_prompt_diagnose_topic_lag_rendering() -> None:
    """验证 diagnose_topic_lag Prompt 渲染模板包含多维诊断排查指导."""
    server = create_mcp_server(KafkaConfig())

    # 1. 渲染带 topic 的 Prompt
    prompt_res = await server.get_prompt(
        "diagnose_topic_lag",
        arguments={"group_id": "analytics-group", "topic": "events"},
    )

    assert len(prompt_res.messages) == 1
    prompt_text = prompt_res.messages[0].content.text

    assert "analytics-group" in prompt_text
    assert "events" in prompt_text
    assert "分区倾斜诊断" in prompt_text
    assert "消费者活跃度与拓扑映射" in prompt_text
    assert "处理耗时与心跳超时" in prompt_text
    assert "kafka_describe_consumer_group" in prompt_text

    # 2. 渲染不带 topic 的 Prompt
    prompt_res_no_topic = await server.get_prompt(
        "diagnose_topic_lag",
        arguments={"group_id": "analytics-group"},
    )
    prompt_text_no_topic = prompt_res_no_topic.messages[0].content.text
    assert "analytics-group" in prompt_text_no_topic
    assert "分区倾斜" in prompt_text_no_topic
