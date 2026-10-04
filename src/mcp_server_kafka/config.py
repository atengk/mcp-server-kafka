"""Kafka MCP 服务配置与参数解析.

@author Ateng
@since 2026-10-04
"""

import argparse
import os

from pydantic import BaseModel, Field


class KafkaConfig(BaseModel):
    """Kafka 服务运行时配置模型.

    @author Ateng
    @since 2026-10-04
    """

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka 集群连接地址 (多个以逗号分隔)",
    )
    read_only: bool = Field(
        default=False,
        description="全局只读防线开关，开启后拒绝一切写操作",
    )
    transport: str = Field(
        default="stdio",
        description="传输协议网关类型 (stdio / sse)",
    )
    host: str = Field(
        default="0.0.0.0",
        description="HTTP SSE 网关监听主机地址",
    )
    port: int = Field(
        default=8000,
        description="HTTP SSE 网关监听端口号",
    )
    log_level: str = Field(
        default="INFO",
        description="服务运行日志级别 (DEBUG / INFO / WARNING / ERROR)",
    )

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """从系统环境变量构建配置实例.

        @return 运行时配置对象
        """
        servers = os.getenv("MCP_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        read_only_raw = os.getenv("MCP_KAFKA_READ_ONLY", "false").strip().lower()
        read_only = read_only_raw in ("true", "1", "yes", "on")

        transport = os.getenv("MCP_KAFKA_TRANSPORT", "stdio").strip().lower()
        if transport not in ("stdio", "sse"):
            transport = "stdio"

        host = os.getenv("MCP_KAFKA_SERVER_HOST", "0.0.0.0").strip()
        port_raw = os.getenv("MCP_KAFKA_SERVER_PORT", "8000").strip()
        try:
            port = int(port_raw)
        except ValueError:
            port = 8000

        log_level = os.getenv("MCP_KAFKA_LOG_LEVEL", "INFO").strip().upper()

        return cls(
            bootstrap_servers=servers,
            read_only=read_only,
            transport=transport,
            host=host,
            port=port,
            log_level=log_level,
        )


def parse_cli_args(args: list[str] | None = None) -> KafkaConfig:
    """解析命令行参数并与环境变量合并.

    @param args 传入的命令行参数列表，为 None 时由 argparse 从 sys.argv 解析
    @return 最终生效的 Kafka 运行时配置
    """
    env_config = KafkaConfig.from_env()

    parser = argparse.ArgumentParser(description="mcp-server-kafka: Apache Kafka MCP 服务端")
    parser.add_argument(
        "--bootstrap-servers",
        default=env_config.bootstrap_servers,
        help="Kafka 集群 Broker 连接地址列表 (默认: 环境变量或 localhost:9092)",
    )
    parser.add_argument(
        "--read-only",
        action="store_true",
        default=env_config.read_only,
        help="启用全局只读模式，屏蔽一切变更类操作",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default=env_config.transport,
        help="传输协议网关类型 (默认: 环境变量或 stdio)",
    )
    parser.add_argument(
        "--host",
        default=env_config.host,
        help="HTTP SSE 网关监听主机地址 (默认: 环境变量或 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=env_config.port,
        help="HTTP SSE 网关监听端口号 (默认: 环境变量或 8000)",
    )
    parser.add_argument(
        "--log-level",
        default=env_config.log_level,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="服务运行日志级别 (默认: 环境变量或 INFO)",
    )

    parsed = parser.parse_args(args)
    return KafkaConfig(
        bootstrap_servers=parsed.bootstrap_servers,
        read_only=parsed.read_only,
        transport=parsed.transport,
        host=parsed.host,
        port=parsed.port,
        log_level=parsed.log_level,
    )
