"""Kafka MCP 服务启动入口与协议路由装配.

@author Ateng
@since 2026-10-04
"""

import logging
import sys
from typing import Any

from mcp.server.mcpserver import MCPServer

from mcp_server_kafka.config import KafkaConfig, parse_cli_args
from mcp_server_kafka.manager import KafkaManager

logger = logging.getLogger(__name__)


def create_mcp_server(
    config: KafkaConfig | None = None,
    manager: KafkaManager | None = None,
) -> MCPServer:
    """构建并装配 Kafka MCP 服务端实例.

    @param config 服务运行时配置，默认从环境变量或预设缺省值加载
    @param manager Kafka 连接管理器实例，传入 None 时基于配置自动实例化
    @return 已装配 Tools 与 Resources 的 MCPServer 实例
    """
    cfg = config or KafkaConfig.from_env()
    mgr = manager or KafkaManager(cfg)

    server = MCPServer(
        name="mcp-server-kafka",
        instructions="Apache Kafka 模型上下文协议 (MCP) 服务端，支持集群元数据只读查询、主题管理与消息排查.",
    )

    # 1. 注册集群元数据查询 Tool
    @server.tool(
        name="kafka_cluster_info",
        description="获取 Kafka 集群整体元数据与 Broker 节点列表（包含 Cluster ID、控制器与节点地址）",
    )
    async def kafka_cluster_info() -> dict[str, Any]:
        """获取 Kafka 集群元数据与 Broker 节点信息.

        @return 包含集群标识符、控制器及节点列表的元数据字典；失败时返回错误描述字典
        """
        try:
            cluster = await mgr.get_cluster_metadata()
            return cluster.model_dump()
        except Exception as exc:  # noqa: BLE001
            logger.warning("获取 Kafka 集群元数据失败: %s", exc)
            return {"error": f"获取集群元数据失败: {exc}"}

    # 2. 注册集群摘要 Resource
    @server.resource(
        "kafka://cluster/summary",
        name="Kafka 集群拓扑摘要",
        description="以只读上下文形式输出当前 Kafka 集群整体元数据与 Broker 运行节点摘要",
        mime_type="text/plain",
    )
    async def get_cluster_summary() -> str:
        """读取 Kafka 集群拓扑只读摘要.

        @return Markdown 格式的集群摘要文本；失败时返回错误描述文本
        """
        try:
            cluster = await mgr.get_cluster_metadata()
            cluster_id_str = cluster.cluster_id or "未知 (未启用 Cluster ID)"
            controller_desc = (
                f"节点 ID {cluster.controller.node_id} ({cluster.controller.host}:{cluster.controller.port})"
                if cluster.controller
                else "未选举或未知"
            )
            broker_lines = [
                f"- Broker ID {broker.node_id}: {broker.host}:{broker.port}"
                + (f" (机架: {broker.rack})" if broker.rack else "")
                for broker in cluster.brokers
            ]
            broker_text = "\n".join(broker_lines) if broker_lines else "暂无可用节点"

            return (
                f"# Kafka 集群概况\n\n"
                f"- **集群标识符 (Cluster ID)**: {cluster_id_str}\n"
                f"- **控制器 (Controller)**: {controller_desc}\n"
                f"- **活跃 Broker 节点数**: {len(cluster.brokers)}\n\n"
                f"## 节点列表\n{broker_text}\n"
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("读取 Kafka 集群摘要失败: %s", exc)
            return f"读取集群摘要失败: {exc}"

    return server


def main() -> None:
    """服务主函数 CLI 运行入口."""
    config = parse_cli_args(sys.argv[1:])
    server = create_mcp_server(config)
    server.run()


if __name__ == "__main__":
    main()
