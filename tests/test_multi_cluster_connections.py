"""多集群命名连接配置、无状态路由与连接级只读保护集成测试.

@author Ateng
@since 2026-10-04
"""

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from mcp_server_kafka.config import parse_cli_args
from mcp_server_kafka.manager import KafkaManager, KafkaManagerRegistry
from mcp_server_kafka.models import (
    BrokerInfo,
    ClusterInfo,
    KafkaConnectionSummary,
)
from mcp_server_kafka.server import create_mcp_server


@pytest.fixture
def temp_connections_yaml(tmp_path: Path) -> Path:
    """创建临时多集群连接 YAML 配置文件.

    @param tmp_path pytest 提供的临时目录路径
    @return 临时 YAML 文件的绝对路径
    """
    yaml_content = """
default_connection: "default"
read_only: false

connections:
  default:
    name: "default"
    bootstrap_servers: "localhost:9092"
    read_only: false

  staging:
    name: "staging"
    bootstrap_servers: "kafka-staging:9092"
    read_only: false

  production:
    name: "production"
    bootstrap_servers: "kafka-prod-1:9092,kafka-prod-2:9092"
    read_only: true
"""
    file_path = tmp_path / "connections.yaml"
    file_path.write_text(yaml_content, encoding="utf-8")
    return file_path


def test_parse_cli_args_with_yaml_config(temp_connections_yaml: Path) -> None:
    """验证从 YAML 配置文件解析多集群配置与合并."""
    config = parse_cli_args(["--config", str(temp_connections_yaml)])

    assert config.config_file == str(temp_connections_yaml)
    assert config.default_connection == "default"
    assert config.read_only is False
    assert len(config.connections) == 3

    assert "default" in config.connections
    assert config.connections["default"].bootstrap_servers == "localhost:9092"
    assert config.connections["default"].read_only is False

    assert "staging" in config.connections
    assert config.connections["staging"].bootstrap_servers == "kafka-staging:9092"
    assert config.connections["staging"].read_only is False

    assert "production" in config.connections
    assert config.connections["production"].bootstrap_servers == "kafka-prod-1:9092,kafka-prod-2:9092"
    assert config.connections["production"].read_only is True


def test_parse_cli_args_missing_file_raises_error() -> None:
    """验证当指定的配置文件不存在时抛出 FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="指定的 Kafka MCP 配置文件不存在"):
        parse_cli_args(["--config", "non_existent_connections.yaml"])


def test_parse_cli_args_override_with_global_read_only(temp_connections_yaml: Path) -> None:
    """验证 CLI 显式 --read-only 强制覆盖全局与所有连接为只读."""
    config = parse_cli_args(["--config", str(temp_connections_yaml), "--read-only"])

    assert config.read_only is True
    # 所有连接均继承全局只读
    for conn in config.connections.values():
        assert conn.read_only is True


def test_registry_routing_and_read_only_checks(temp_connections_yaml: Path) -> None:
    """验证连接注册中心的动态路由与连接级只读校验."""
    config = parse_cli_args(["--config", str(temp_connections_yaml)])
    registry = KafkaManagerRegistry(config)

    # 1. 默认连接路由
    default_mgr = registry.get_manager()
    assert default_mgr.config.bootstrap_servers == "localhost:9092"
    assert registry.is_connection_read_only("default") is False

    # 2. 指定别名路由
    staging_mgr = registry.get_manager("staging")
    assert staging_mgr.config.bootstrap_servers == "kafka-staging:9092"
    assert registry.is_connection_read_only("staging") is False

    # 3. 生产连接只读保护判断
    prod_mgr = registry.get_manager("production")
    assert prod_mgr.config.bootstrap_servers == "kafka-prod-1:9092,kafka-prod-2:9092"
    assert registry.is_connection_read_only("production") is True

    # 4. 未知连接别名抛出语义明确异常
    with pytest.raises(ValueError, match="未找到名为 'unknown_cluster' 的 Kafka 连接配置"):
        registry.get_manager("unknown_cluster")

    # 5. 枚举连接清单
    summaries = registry.list_connections()
    assert len(summaries) == 3
    summary_map: dict[str, KafkaConnectionSummary] = {s.name: s for s in summaries}
    assert summary_map["default"].is_default is True
    assert summary_map["default"].read_only is False
    assert summary_map["production"].is_default is False
    assert summary_map["production"].read_only is True


@pytest.mark.asyncio
async def test_tool_kafka_list_connections(temp_connections_yaml: Path) -> None:
    """验证 kafka_list_connections 工具输出所有集群连接元数据."""
    config = parse_cli_args(["--config", str(temp_connections_yaml)])
    registry = KafkaManagerRegistry(config)
    server = create_mcp_server(config, registry=registry)

    result = await server.call_tool("kafka_list_connections", {})
    assert result.is_error is False

    data: dict[str, Any] = json.loads(result.content[0].text)
    assert data["default_connection"] == "default"
    assert data["count"] == 3
    conn_names = [c["name"] for c in data["connections"]]
    assert "default" in conn_names
    assert "staging" in conn_names
    assert "production" in conn_names


@pytest.mark.asyncio
async def test_tool_dynamic_routing_to_specific_cluster(temp_connections_yaml: Path) -> None:
    """验证在工具调用中传入 connection 参数将请求正确路由至目标集群."""
    config = parse_cli_args(["--config", str(temp_connections_yaml)])
    registry = KafkaManagerRegistry(config)

    # Mock default 和 staging 管理器
    mock_default_mgr = MagicMock(spec=KafkaManager)
    mock_default_mgr.get_cluster_metadata = AsyncMock(
        return_value=ClusterInfo(
            cluster_id="default-cluster",
            controller=BrokerInfo(node_id=1, host="default-host", port=9092),
            brokers=[BrokerInfo(node_id=1, host="default-host", port=9092)],
        )
    )

    mock_staging_mgr = MagicMock(spec=KafkaManager)
    mock_staging_mgr.get_cluster_metadata = AsyncMock(
        return_value=ClusterInfo(
            cluster_id="staging-cluster",
            controller=BrokerInfo(node_id=10, host="staging-host", port=9092),
            brokers=[BrokerInfo(node_id=10, host="staging-host", port=9092)],
        )
    )

    registry._managers["default"] = mock_default_mgr
    registry._managers["staging"] = mock_staging_mgr

    server = create_mcp_server(config, registry=registry)

    # 1. 默认调用 -> 路由至 default
    res_default = await server.call_tool("kafka_cluster_info", {})
    assert res_default.is_error is False
    data_default: dict[str, Any] = json.loads(res_default.content[0].text)
    assert data_default["cluster_id"] == "default-cluster"
    assert mock_default_mgr.get_cluster_metadata.called

    # 2. 传参 connection='staging' -> 路由至 staging
    res_staging = await server.call_tool("kafka_cluster_info", {"connection": "staging"})
    assert res_staging.is_error is False
    data_staging: dict[str, Any] = json.loads(res_staging.content[0].text)
    assert data_staging["cluster_id"] == "staging-cluster"
    assert mock_staging_mgr.get_cluster_metadata.called


@pytest.mark.asyncio
async def test_per_connection_read_only_guard(temp_connections_yaml: Path) -> None:
    """验证连接级细粒度只读保护：对只读连接执行写操作被精确拦截."""
    config = parse_cli_args(["--config", str(temp_connections_yaml)])
    registry = KafkaManagerRegistry(config)

    mock_staging_mgr = MagicMock(spec=KafkaManager)
    mock_staging_mgr.create_topic = AsyncMock(return_value={"status": "created", "topic": "test-topic"})
    registry._managers["staging"] = mock_staging_mgr

    mock_prod_mgr = MagicMock(spec=KafkaManager)
    registry._managers["production"] = mock_prod_mgr

    server = create_mcp_server(config, registry=registry)

    # 1. 对 staging（可写）执行创建主题 -> 成功放行
    res_staging = await server.call_tool(
        "kafka_create_topic",
        {"topic_name": "test-topic", "connection": "staging"},
    )
    assert res_staging.is_error is False
    data_staging: dict[str, Any] = json.loads(res_staging.content[0].text)
    assert data_staging["status"] == "created"
    assert mock_staging_mgr.create_topic.called

    # 2. 对 production（只读）执行创建主题 -> 精准拦截拒绝
    res_prod = await server.call_tool(
        "kafka_create_topic",
        {"topic_name": "danger-topic", "connection": "production"},
    )
    assert res_prod.is_error is False
    data_prod: dict[str, Any] = json.loads(res_prod.content[0].text)
    assert "error" in data_prod
    assert "只读保护模式" in data_prod["error"]
    assert not mock_prod_mgr.create_topic.called
