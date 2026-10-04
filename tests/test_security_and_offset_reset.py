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

    # 1. 尝试对连接级只读的 prod 集群执行真实重置 (dry_run=False) -> 拦截
    res_ro = await server.call_tool(
        "kafka_reset_consumer_group_offsets",
        {
            "group_id": "test_grp",
            "topic": "test_topic",
            "connection": "prod",
            "dry_run": False,
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


@pytest.mark.asyncio
async def test_reset_consumer_group_offsets_upper_bound_guard() -> None:
    """验证 to_offset 策略遇到目标位移超越 LEO 时的原子性上界防御拦截 (Offset Upper Bound Guard)."""
    cfg = KafkaConnectionConfig(name="test_conn")
    mgr = KafkaManager(cfg)

    tp0 = TopicPartition("order-topic", 0)
    tp1 = TopicPartition("order-topic", 1)

    mock_admin = AsyncMock()
    mock_admin.describe_consumer_groups = AsyncMock(
        return_value=[MagicMock(state="Empty", members=[])]
    )
    mock_admin.list_consumer_group_offsets = AsyncMock(return_value={})
    mgr._admin_client = mock_admin

    mock_consumer = AsyncMock()
    mock_consumer.partitions_for_topic = AsyncMock(return_value={0, 1})
    # 分区 0 的 LEO 为 500，分区 1 的 LEO 仅为 100
    mock_consumer.end_offsets = AsyncMock(return_value={tp0: 500, tp1: 100})

    with patch.object(mgr, "create_consumer", return_value=mock_consumer):
        # 1. 传入 offset=200，超过了分区 1 的 LEO (100) -> 触发整体验证原子性拦截
        with pytest.raises(ValueError, match="超出分区日志末端位移 \\(LEO\\) 上界，已触发原子性拦截防御") as exc_info:
            await mgr.reset_consumer_group_offsets(
                group_id="order_group",
                topic="order-topic",
                strategy="to_offset",
                offset=200,
                dry_run=True,
            )
        err_msg = str(exc_info.value)
        assert "分区 1 (当前 LEO: 100)" in err_msg
        assert "严禁设置未来位移以防消息永久丢失" in err_msg


@pytest.mark.asyncio
async def test_tool_kafka_reset_consumer_group_offsets_in_global_read_only() -> None:
    """验证在全局只读模式下位移重置工具依然开放注册，支持安全的 dry_run=True 预检并拦截物理提交."""
    config = KafkaConfig(
        default_connection="default",
        connections={
            "default": KafkaConnectionConfig(name="default", read_only=False),
        },
        read_only=True,  # 全局只读
    )
    registry = KafkaManagerRegistry(config)
    server = create_mcp_server(config, registry=registry)

    # 1. 校验工具已被成功注册
    tools = await server.list_tools()
    tool_names = [t.name for t in tools]
    assert "kafka_reset_consumer_group_offsets" in tool_names

    # 2. 校验工具描述动态注入了只读模式引导提示
    reset_tool = next(t for t in tools if t.name == "kafka_reset_consumer_group_offsets")
    assert "【只读模式：仅支持 dry_run=True 预检评估，物理提交将被严格拦截】" in reset_tool.description

    # 3. 尝试在只读模式下执行物理提交 (dry_run=False) -> 拦截并友好提示
    res = await server.call_tool(
        "kafka_reset_consumer_group_offsets",
        {
            "group_id": "test_grp",
            "topic": "test_topic",
            "dry_run": False,
            "confirm": True,
        },
    )
    data = json.loads(res.content[0].text)
    assert "error" in data
    assert "已配置为只读保护模式，禁止执行物理位移重置提交" in data["error"]


@pytest.mark.asyncio
async def test_produce_message_with_non_string_headers() -> None:
    """验证生产消息时传入非字符串标头（如 int/bool）能被安全编码为 utf-8 字节，避免 TypeError 崩溃."""
    cfg = KafkaConnectionConfig(name="test_conn")
    mgr = KafkaManager(cfg)

    mock_producer = AsyncMock()
    mock_meta = MagicMock(topic="order-topic", partition=0, offset=42, timestamp=1728045600000)
    mock_producer.send_and_wait = AsyncMock(return_value=mock_meta)
    mgr._producer = mock_producer

    result = await mgr.produce_message(
        topic="order-topic",
        value={"status": "ok"},
        headers={"retry_count": 3, "is_valid": True, "trace_id": "tr-999"},
    )
    assert result.offset == 42
    # 验证传入底层的 headers 格式为合法的 list[tuple[str, bytes]]
    assert mock_producer.send_and_wait.called
    call_kwargs = mock_producer.send_and_wait.call_args.kwargs
    sent_headers = call_kwargs["headers"]
    assert ("retry_count", b"3") in sent_headers
    assert ("is_valid", b"True") in sent_headers
    assert ("trace_id", b"tr-999") in sent_headers
