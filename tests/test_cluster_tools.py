"""集群元数据只读工具与资源接口集成测试 (最高测试接缝).

@author Ateng
@since 2026-10-04
"""

import json
from typing import Any

import pytest

from mcp_server_kafka.config import KafkaConfig
from mcp_server_kafka.manager import KafkaManager
from mcp_server_kafka.models import BrokerInfo, ClusterInfo
from mcp_server_kafka.server import create_mcp_server


class FakeKafkaManager(KafkaManager):
    """用于测试接缝注入的 Kafka 桩管理器.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self, cluster_info: ClusterInfo | None = None, raise_error: Exception | None = None) -> None:
        super().__init__(KafkaConfig(bootstrap_servers="mock:9092"))
        self._cluster_info = cluster_info or ClusterInfo(
            cluster_id="fake-cluster-123",
            controller=BrokerInfo(node_id=1, host="node-1", port=9092, rack="zone-a"),
            brokers=[
                BrokerInfo(node_id=1, host="node-1", port=9092, rack="zone-a"),
                BrokerInfo(node_id=2, host="node-2", port=9092, rack="zone-b"),
            ],
        )
        self._raise_error = raise_error

    async def get_cluster_metadata(self) -> ClusterInfo:
        """返回预设的集群元数据或模拟异常."""
        if self._raise_error:
            raise self._raise_error
        return self._cluster_info


@pytest.mark.asyncio
async def test_tool_kafka_cluster_info_success() -> None:
    """验证在最高测试接缝下调用 kafka_cluster_info 工具返回完整元数据."""
    manager = FakeKafkaManager()
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 1. 在协议层调用 Tool
    result = await server.call_tool("kafka_cluster_info", {})

    # 2. 验证响应契约
    assert result.is_error is False
    assert len(result.content) >= 1
    raw_text = result.content[0].text
    data: dict[str, Any] = json.loads(raw_text)

    assert data["cluster_id"] == "fake-cluster-123"
    assert data["controller"]["node_id"] == 1
    assert data["controller"]["host"] == "node-1"
    assert len(data["brokers"]) == 2
    assert data["brokers"][0]["node_id"] == 1
    assert data["brokers"][1]["node_id"] == 2


@pytest.mark.asyncio
async def test_resource_kafka_cluster_summary_success() -> None:
    """验证在协议层读取 kafka://cluster/summary 资源输出摘要上下文."""
    manager = FakeKafkaManager()
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 1. 在协议层读取 Resource
    resources = await server.read_resource("kafka://cluster/summary")

    # 2. 验证摘要内容包含关键信息
    assert len(resources) >= 1
    content = resources[0].content
    assert "fake-cluster-123" in content
    assert "node-1" in content
    assert "node-2" in content


@pytest.mark.asyncio
async def test_tool_kafka_cluster_info_connection_failure() -> None:
    """验证集群连接异常时的防御与语义化报错."""
    manager = FakeKafkaManager(raise_error=ConnectionError("无法连接到 Kafka Broker 集群"))
    server = create_mcp_server(KafkaConfig(), manager=manager)

    # 验证在协议层调用 Tool 时捕获连接错误并返回优雅提示
    result = await server.call_tool("kafka_cluster_info", {})
    assert result.is_error is True or "无法连接" in result.content[0].text
