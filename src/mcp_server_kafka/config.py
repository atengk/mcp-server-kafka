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

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """从系统环境变量构建配置实例.

        @return 运行时配置对象
        """
        servers = os.getenv("MCP_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        read_only_raw = os.getenv("MCP_KAFKA_READ_ONLY", "false").strip().lower()
        read_only = read_only_raw in ("true", "1", "yes", "on")
        return cls(bootstrap_servers=servers, read_only=read_only)


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

    parsed, _ = parser.parse_known_args(args)
    return KafkaConfig(
        bootstrap_servers=parsed.bootstrap_servers,
        read_only=parsed.read_only,
    )
