"""传输协议双模网关与配置解析集成测试 (最高测试接缝).

@author Ateng
@since 2026-10-04
"""

from typing import Any
from unittest.mock import MagicMock

import pytest

from mcp_server_kafka.config import KafkaConfig, parse_cli_args
from mcp_server_kafka.server import create_mcp_server, run_server


def test_config_transport_defaults() -> None:
    """验证传输网关与监听端口的预设缺省值."""
    cfg = KafkaConfig()
    assert cfg.transport == "stdio"
    assert cfg.host == "0.0.0.0"
    assert cfg.port == 8000
    assert cfg.log_level == "INFO"


def test_config_transport_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证从系统环境变量加载传输协议、监听主机与端口."""
    monkeypatch.setenv("MCP_KAFKA_TRANSPORT", "sse")
    monkeypatch.setenv("MCP_KAFKA_SERVER_HOST", "127.0.0.1")
    monkeypatch.setenv("MCP_KAFKA_SERVER_PORT", "9090")
    monkeypatch.setenv("MCP_KAFKA_LOG_LEVEL", "DEBUG")

    cfg = KafkaConfig.from_env()
    assert cfg.transport == "sse"
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 9090
    assert cfg.log_level == "DEBUG"


def test_parse_cli_args_transport_override() -> None:
    """验证命令行参数对传输协议与网络参数的高优先级覆盖."""
    args = [
        "--transport",
        "sse",
        "--host",
        "0.0.0.0",
        "--port",
        "8008",
        "--log-level",
        "WARNING",
    ]
    cfg = parse_cli_args(args)
    assert cfg.transport == "sse"
    assert cfg.host == "0.0.0.0"
    assert cfg.port == 8008
    assert cfg.log_level == "WARNING"


def test_parse_cli_args_invalid_transport() -> None:
    """验证非法传输协议参数触发系统校验防御."""
    with pytest.raises((ValueError, SystemExit)):
        parse_cli_args(["--transport", "unsupported-proto"])


def test_run_server_stdio_dispatch() -> None:
    """验证 Stdio 模式下服务调度分发逻辑."""
    cfg = KafkaConfig(transport="stdio")
    mock_server = MagicMock()

    run_server(cfg, server=mock_server)
    mock_server.run.assert_called_once_with(transport="stdio")


def test_run_server_sse_dispatch() -> None:
    """验证 SSE 模式下携带网络监听参数调度分发逻辑."""
    cfg = KafkaConfig(transport="sse", host="127.0.0.1", port=8000)
    mock_server = MagicMock()

    run_server(cfg, server=mock_server)
    mock_server.run.assert_called_once_with(transport="sse", host="127.0.0.1", port=8000)


def test_sse_app_endpoints_mount() -> None:
    """验证 FastMCP 生成的 SSE ASGI 应用挂载标准路由."""
    server = create_mcp_server(KafkaConfig())
    app: Any = server.sse_app()
    route_paths = [r.path for r in app.routes]

    assert "/sse" in route_paths
    assert any("/messages" in r for r in route_paths)


@pytest.mark.asyncio
async def test_prompt_inspect_topic_messages_rendering() -> None:
    """验证 inspect_topic_messages Prompt 渲染模板包含采样排查指引."""
    server = create_mcp_server(KafkaConfig())
    prompt_res = await server.get_prompt(
        "inspect_topic_messages",
        arguments={"topic": "billing-events", "limit": "20"},
    )
    assert len(prompt_res.messages) == 1
    text = prompt_res.messages[0].content.text
    assert "billing-events" in text
    assert "kafka_sample_messages" in text
    assert "零位移安全采样读取" in text

