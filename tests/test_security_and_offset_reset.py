"""企业级安全认证协议矩阵与消费组位移治理集成测试.

验证环境变量插值引擎、自适应 SSL 上下文构建、位移重置计算模型、
Dry-Run 预检试运行及活跃消费组安全防御.

@author Ateng
@since 2026-10-04
"""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka import TopicPartition
from aiokafka.structs import OffsetAndMetadata

from mcp_server_kafka.config import (
    KafkaConfig,
    KafkaConnectionConfig,
    interpolate_env_vars,
)
from mcp_server_kafka.manager import (
    KafkaManager,
    KafkaManagerRegistry,
    _parse_datetime_to_ms,
    build_ssl_context,
)
from mcp_server_kafka.server import create_mcp_server


def test_interpolate_env_vars_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证环境变量插值引擎在字符串、字典与嵌套列表中成功提取替换."""
    monkeypatch.setenv("TEST_KAFKA_USER", "operator_ateng")
    monkeypatch.setenv("TEST_KAFKA_PASS", "SuperSecurePass123!")

    raw_data: dict[str, Any] = {
        "user": "${TEST_KAFKA_USER}",
        "auth": {
            "password": "${TEST_KAFKA_PASS}",
            "tokens": ["${TEST_KAFKA_USER}", "static_token"],
        },
        "port": 9092,
    }

    interpolated = interpolate_env_vars(raw_data)
    assert interpolated["user"] == "operator_ateng"
    assert interpolated["auth"]["password"] == "SuperSecurePass123!"
    assert interpolated["auth"]["tokens"] == ["operator_ateng", "static_token"]
    assert interpolated["port"] == 9092


def test_interpolate_env_vars_missing_raises_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证当引用的环境变量未定义时实行 Fail-Fast 严格报错."""
    monkeypatch.delenv("NON_EXISTENT_VAR_12345", raising=False)
    with pytest.raises(ValueError, match="在当前系统中未定义"):
        interpolate_env_vars("${NON_EXISTENT_VAR_12345}")


def test_build_ssl_context_logic() -> None:
    """验证自适应 SSLContext 构建器在不同安全协议下的行为."""
    # 1. PLAINTEXT 返回 None
    cfg_plain = KafkaConnectionConfig(name="plain", security_protocol="PLAINTEXT")
    assert build_ssl_context(cfg_plain) is None

    # 2. SSL 默认上下文构建
    cfg_ssl = KafkaConnectionConfig(
        name="ssl_cluster",
        security_protocol="SSL",
        ssl_check_hostname=False,
    )
    ctx = build_ssl_context(cfg_ssl)
    assert ctx is not None
    assert ctx.check_hostname is False


def test_parse_datetime_to_ms() -> None:
    """验证时间输入解析引擎对数字与 ISO-8601 字符串的双模兼容性."""
    # 毫秒与秒级数字
    assert _parse_datetime_to_ms(1700000000) == 1700000000000
    assert _parse_datetime_to_ms(1700000000123) == 1700000000123

    # ISO-8601 字符串
    iso_str = "2026-10-04T12:00:00+00:00"
    ms = _parse_datetime_to_ms(iso_str)
    assert isinstance(ms, int)
    assert ms > 0


@pytest.mark.asyncio
async def test_reset_consumer_group_offsets_dry_run() -> None:
    """验证消费位移重置在 dry_run=True 预检模式下的位移差值计算且不执行落盘."""
    cfg = KafkaConnectionConfig(name="test_conn")
    mgr = KafkaManager(cfg)

    tp0 = TopicPartition("order-topic", 0)
    tp1 = TopicPartition("order-topic", 1)

    # Mock AdminClient
    mock_admin = AsyncMock()
    mock_admin.describe_consumer_groups = AsyncMock(
        return_value=[MagicMock(state="Empty", members=[])]
    )
    mock_admin.list_consumer_group_offsets = AsyncMock(
        return_value={
            tp0: OffsetAndMetadata(100, ""),
            tp1: OffsetAndMetadata(200, ""),
        }
    )
    mgr._admin_client = mock_admin

    # Mock Consumer
    mock_consumer = AsyncMock()
    mock_consumer.partitions_for_topic = AsyncMock(return_value={0, 1})
    mock_consumer.beginning_offsets = AsyncMock(return_value={tp0: 10, tp1: 20})
    mock_consumer.end_offsets = AsyncMock(return_value={tp0: 150, tp1: 250})

    with patch.object(mgr, "create_consumer", return_value=mock_consumer):
        # 1. 执行 earliest 策略预检
        res = await mgr.reset_consumer_group_offsets(
            group_id="order_group",
            topic="order-topic",
            strategy="earliest",
            dry_run=True,
        )

        assert res.group_id == "order_group"
        assert res.topic == "order-topic"
        assert res.strategy == "earliest"
        assert res.dry_run is True
        assert res.applied is False
        assert res.total_partitions == 2

        part_map = {p.partition: p for p in res.partitions}
        assert part_map[0].current_offset == 100
        assert part_map[0].target_offset == 10
        assert part_map[0].offset_delta == -90

        assert part_map[1].current_offset == 200
        assert part_map[1].target_offset == 20
        assert part_map[1].offset_delta == -180

        assert res.total_delta == -270


@pytest.mark.asyncio
async def test_reset_consumer_group_offsets_active_group_guard() -> None:
    """验证当消费组处于 Stable 活跃状态时执行前置冲突拦截与 force 参数放行."""
    cfg = KafkaConnectionConfig(name="test_conn")
    mgr = KafkaManager(cfg)

    # Mock 活跃状态的消费组 (含有 2 个活跃成员)
    mock_member = MagicMock(member_id="consumer-client-1")
    mock_grp = MagicMock(state="Stable", members=[mock_member, mock_member])

    mock_admin = AsyncMock()
    mock_admin.describe_consumer_groups = AsyncMock(return_value=[mock_grp])
    mock_admin.list_consumer_group_offsets = AsyncMock(return_value={})
    mgr._admin_client = mock_admin

    mock_consumer = AsyncMock()
    mock_consumer.partitions_for_topic = AsyncMock(return_value={0})
    mock_consumer.end_offsets = AsyncMock(
        return_value={TopicPartition("order-topic", 0): 100}
    )

    with patch.object(mgr, "create_consumer", return_value=mock_consumer):
        # 1. 非 dry_run 且未传 force=True 时，必须拦截抛出异常
        with pytest.raises(ValueError, match="当前处于活跃运行状态"):
            await mgr.reset_consumer_group_offsets(
                group_id="active_group",
                topic="order-topic",
                strategy="latest",
                dry_run=False,
                force=False,
            )

        # 2. dry_run=True 时放行并附带 warning
        res_dry = await mgr.reset_consumer_group_offsets(
            group_id="active_group",
            topic="order-topic",
            strategy="latest",
            dry_run=True,
            force=False,
        )
        assert res_dry.warning is not None
        assert "当前处于活跃运行状态" in res_dry.warning


@pytest.mark.asyncio
async def test_tool_kafka_reset_consumer_group_offsets_safeties() -> None:
    """验证 Tool 层在全局只读、未传 confirm 与正常调用的安全防线."""
    config = KafkaConfig(
        default_connection="default",
        connections={
            "default": KafkaConnectionConfig(name="default", read_only=False),
            "prod": KafkaConnectionConfig(name="prod", read_only=True),
        },
        read_only=False,
    )
    registry = KafkaManagerRegistry(config)
    server = create_mcp_server(config, registry=registry)

    # 1. 尝试对连接级只读的 prod 集群执行重置 -> 拦截
    res_ro = await server.call_tool(
        "kafka_reset_consumer_group_offsets",
        {
            "group_id": "test_grp",
            "topic": "test_topic",
            "connection": "prod",
        },
    )
    data_ro = json.loads(res_ro.content[0].text)
    assert "error" in data_ro
    assert "只读保护模式" in data_ro["error"]

    # 2. 执行落盘 (dry_run=False) 但未确认 (confirm=False) -> 拦截
    res_confirm = await server.call_tool(
        "kafka_reset_consumer_group_offsets",
        {
            "group_id": "test_grp",
            "topic": "test_topic",
            "dry_run": False,
            "confirm": False,
        },
    )
    data_confirm = json.loads(res_confirm.content[0].text)
    assert "error" in data_confirm
    assert "必须同时显式传入 dry_run=False 和 confirm=True" in data_confirm["error"]
