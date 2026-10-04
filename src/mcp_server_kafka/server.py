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

    # 2. 注册主题列表查询 Tool
    @server.tool(
        name="kafka_list_topics",
        description="列出 Kafka 集群中的主题清单（支持按名称模式过滤并默认排除内部系统主题）",
    )
    async def kafka_list_topics(
        pattern: str | None = None,
        include_internal: bool = False,
    ) -> dict[str, Any]:
        """列出 Kafka 集群中的主题摘要信息.

        @param pattern 可选名称模式过滤字符串（模糊匹配）
        @param include_internal 是否包含系统内部主题 (如 __consumer_offsets)
        @return 包含主题清单 topics 与总数 count 的结果字典；失败时返回错误描述字典
        """
        try:
            summaries = await mgr.list_topics(pattern=pattern, include_internal=include_internal)
            return {
                "topics": [s.model_dump() for s in summaries],
                "count": len(summaries),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("获取 Kafka 主题列表失败: %s", exc)
            return {"error": f"获取主题列表失败: {exc}"}

    # 3. 注册主题详细信息查询 Tool
    @server.tool(
        name="kafka_describe_topic",
        description="查询指定 Kafka 主题的详细拓扑（分区分布、Leader 节点、ISR 同步副本与配置）",
    )
    async def kafka_describe_topic(topic_name: str) -> dict[str, Any]:
        """查询指定 Kafka 主题详细信息.

        @param topic_name 目标主题名称
        @return 包含分区列表与自定义配置的主题详情字典
        """
        try:
            detail = await mgr.describe_topic(topic_name=topic_name)
            return detail.model_dump()
        except Exception as exc:  # noqa: BLE001
            logger.warning("查询 Kafka 主题详情失败 [topic=%s]: %s", topic_name, exc)
            return {"error": f"查询主题详情失败: {exc}"}

    # 4. 注册写操作 Tools (双重安全防线之一：只读模式隐藏拦截)
    if not cfg.read_only:

        @server.tool(
            name="kafka_create_topic",
            description="在 Kafka 集群中创建新主题（支持指定分区数与副本因子；只读模式下不可用）",
        )
        async def kafka_create_topic(
            topic_name: str,
            partitions: int = 1,
            replication_factor: int = 1,
        ) -> dict[str, Any]:
            """创建新的 Kafka 主题.

            @param topic_name 待创建的主题名称
            @param partitions 分区数量，默认 1
            @param replication_factor 副本因子，默认 1
            @return 创建状态结果描述
            """
            try:
                return await mgr.create_topic(
                    topic_name=topic_name,
                    partitions=partitions,
                    replication_factor=replication_factor,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("创建 Kafka 主题失败 [topic=%s]: %s", topic_name, exc)
                return {"error": f"创建主题失败: {exc}"}

        @server.tool(
            name="kafka_delete_topic",
            description="从 Kafka 集群中删除主题（高危破坏操作，必须传入 confirm=True 显式确认）",
        )
        async def kafka_delete_topic(
            topic_name: str,
            confirm: bool = False,
        ) -> dict[str, Any]:
            """删除指定的 Kafka 主题.

            @param topic_name 待删除的主题名称
            @param confirm 破坏性操作显式确认标志，必须为 True 方可执行
            @return 删除状态结果描述
            """
            # 二级防线：破坏性动作显式二次确认校验
            if not confirm:
                return {
                    "error": "删除主题属于高危破坏性操作，必须显式传入 confirm=True 二次确认以防数据丢失",
                }
            try:
                return await mgr.delete_topic(topic_name=topic_name)
            except Exception as exc:  # noqa: BLE001
                logger.warning("删除 Kafka 主题失败 [topic=%s]: %s", topic_name, exc)
                return {"error": f"删除主题失败: {exc}"}

    # 5. 注册集群摘要 Resource
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

    # 6. 注册指定主题动态 Resource
    @server.resource(
        "kafka://topics/{topic}",
        name="Kafka 主题拓扑与分区明细",
        description="以只读上下文形式输出指定 Kafka 主题的分区、Leader 与副本分布",
        mime_type="text/plain",
    )
    async def get_topic_detail_resource(topic: str) -> str:
        """读取指定 Kafka 主题的动态详情上下文.

        @param topic 目标主题名称
        @return Markdown 格式的主题分区与副本拓扑明细
        """
        try:
            detail = await mgr.describe_topic(topic_name=topic)
            lines = [
                f"# 主题拓扑详情: {detail.name}\n",
                f"- **是否内部主题**: {'是' if detail.is_internal else '否'}",
                f"- **物理分区总数**: {len(detail.partitions)}\n",
                "## 分区明细",
            ]
            for p in detail.partitions:
                leader_str = f"Broker {p.leader}" if p.leader is not None else "无"
                lines.append(
                    f"- **分区 {p.partition_id}**: Leader={leader_str}, 副本={p.replicas}, ISR={p.isr}"
                )
            return "\n".join(lines)
        except Exception as exc:  # noqa: BLE001
            logger.warning("读取 Kafka 主题资源失败 [topic=%s]: %s", topic, exc)
            return f"读取主题 '{topic}' 详情失败: {exc}"

    return server


def main() -> None:
    """服务主函数 CLI 运行入口."""
    config = parse_cli_args(sys.argv[1:])
    server = create_mcp_server(config)
    server.run()


if __name__ == "__main__":
    main()
