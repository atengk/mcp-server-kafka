"""配置解析与环境参数单元测试.

@author Ateng
@since 2026-10-04
"""

import os
from unittest import mock

import pytest

from mcp_server_kafka.config import (
    KafkaConfig,
    KafkaConnectionConfig,
    interpolate_env_vars,
    parse_cli_args,
)
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


def test_env_var_interpolation_with_default() -> None:
    """验证 ${VAR:-default} 环境变量默认值插值语法."""
    # 1. 变量未设置，使用默认值
    with mock.patch.dict(os.environ, {}, clear=True):
        res = interpolate_env_vars("${KAFKA_HOST:-127.0.0.1:9092}")
        assert res == "127.0.0.1:9092"

    # 2. 变量已设置，使用实际值
    with mock.patch.dict(os.environ, {"KAFKA_HOST": "prod.kafka:9094"}):
        res = interpolate_env_vars("${KAFKA_HOST:-127.0.0.1:9092}")
        assert res == "prod.kafka:9094"

    # 3. 变量未设置且无默认值，严格 Fail-Fast 抛出 ValueError
    with mock.patch.dict(os.environ, {}, clear=True), pytest.raises(ValueError, match="在当前系统中未定义"):
        interpolate_env_vars("${UNDEFINED_ENV_VAR}")


def test_ssl_certificate_fail_fast_validation(tmp_path: pytest.TempPathFactory) -> None:
    """验证 SSL 证书与密钥路径不存在时严格 Fail-Fast 抛出 FileNotFoundError."""
    # 1. cafile 不存在
    with pytest.raises(FileNotFoundError, match="指定的 SSL CA 根证书文件不存在"):
        KafkaConnectionConfig(
            name="test",
            ssl_cafile=str(tmp_path / "non_existent_ca.crt"),
        )

    # 2. certfile 不存在
    with pytest.raises(FileNotFoundError, match="指定的 SSL 客户端证书文件不存在"):
        KafkaConnectionConfig(
            name="test",
            ssl_certfile=str(tmp_path / "non_existent_client.crt"),
            ssl_keyfile=str(tmp_path / "any.key"),
        )

    # 3. 配置了 certfile 但未配置 keyfile
    real_cert = tmp_path / "client.crt"
    real_cert.write_text("dummy-cert")
    with pytest.raises(ValueError, match="必须成对配置 ssl_keyfile"):
        KafkaConnectionConfig(
            name="test",
            ssl_certfile=str(real_cert),
        )

