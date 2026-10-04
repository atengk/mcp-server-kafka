"""配置解析与环境参数单元测试.

@author Ateng
@since 2026-10-04
"""

import os
from unittest import mock

from mcp_server_kafka.config import KafkaConfig, parse_cli_args
from mcp_server_kafka.models import BrokerInfo, ClusterInfo


def test_default_config() -> None:
    """验证默认配置参数."""
    config = KafkaConfig.from_env()
    assert config.bootstrap_servers == "localhost:9092"
    assert config.read_only is False


def test_config_from_env() -> None:
    """验证从环境变量解析配置."""
    env = {
        "MCP_KAFKA_BOOTSTRAP_SERVERS": "kafka.internal:9092",
        "MCP_KAFKA_READ_ONLY": "true",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = KafkaConfig.from_env()
        assert config.bootstrap_servers == "kafka.internal:9092"
        assert config.read_only is True


def test_parse_cli_args() -> None:
    """验证从 CLI 命令行参数解析配置."""
    args = ["--bootstrap-servers", "broker1:9092,broker2:9092", "--read-only"]
    config = parse_cli_args(args)
    assert config.bootstrap_servers == "broker1:9092,broker2:9092"
    assert config.read_only is True


def test_cluster_models() -> None:
    """验证集群与节点元数据模型契约."""
    broker1 = BrokerInfo(node_id=1, host="node1", port=9092, rack="rack-1")
    cluster = ClusterInfo(
        cluster_id="test-cluster-id",
        controller=broker1,
        brokers=[broker1],
    )
    assert cluster.cluster_id == "test-cluster-id"
    assert len(cluster.brokers) == 1
    assert cluster.brokers[0].host == "node1"
    assert cluster.controller is not None
    assert cluster.controller.node_id == 1
