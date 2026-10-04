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

    # 4. 注册消息采样 Tool (零提交位移只读探查)
    @server.tool(
        name="kafka_sample_messages",
        description="从指定 Kafka 主题以零提交位移方式只读采样消息（支持 latest/earliest/offset 策略与自适应解码）",
    )
    async def kafka_sample_messages(
        topic: str,
        partition: int | None = None,
        strategy: str = "latest",
        offset: int | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """以零位移影响方式采样拉取主题消息.

        @param topic 目标主题名称
        @param partition 可选指定物理分区
        @param strategy 采样策略 (latest / earliest / offset)，默认 latest
        @param offset 当 strategy=offset 时的起始数值
        @param limit 采样条数限制，默认 10，上限 100
        @return 包含 messages 列表与 total 总数的结果字典；失败时返回错误描述字典
        """
        try:
            sampled = await mgr.sample_messages(
                topic=topic,
                partition=partition,
                strategy=strategy,
                offset=offset,
                limit=limit,
            )
            return {
                "topic": topic,
                "messages": [m.model_dump() for m in sampled],
                "total": len(sampled),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("采样 Kafka 消息失败 [topic=%s]: %s", topic, exc)
            return {"error": f"采样消息失败: {exc}"}

    # 5. 注册消费组列表查询 Tool
    @server.tool(
        name="kafka_list_consumer_groups",
        description="列出 Kafka 集群中的消费组清单（包含 Group ID、协议类型与活跃状态如 Stable/Empty/Dead）",
    )
    async def kafka_list_consumer_groups() -> dict[str, Any]:
        """列出 Kafka 集群中的消费组摘要信息.

        @return 包含消费组清单 groups 与总数 count 的结果字典；失败时返回错误描述字典
        """
        try:
            groups = await mgr.list_consumer_groups()
            return {
                "groups": [g.model_dump() for g in groups],
                "count": len(groups),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("获取 Kafka 消费组列表失败: %s", exc)
            return {"error": f"获取消费组列表失败: {exc}"}

    # 6. 注册消费组详情与积压分析 Tool
    @server.tool(
        name="kafka_describe_consumer_group",
        description="查询指定 Kafka 消费组的详细拓扑，包含活跃成员、分区 Committed Offset、LEO 及 Lag 积压数值",
    )
    async def kafka_describe_consumer_group(group_id: str) -> dict[str, Any]:
        """查询指定 Kafka 消费组详细拓扑与分区积压.

        @param group_id 目标消费组 ID
        @return 包含活跃成员分配及各分区 Lag 积压明细的详情字典；失败时返回错误描述字典
        """
        try:
            detail = await mgr.describe_consumer_group(group_id=group_id)
            return detail.model_dump()
        except Exception as exc:  # noqa: BLE001
            logger.warning("查询 Kafka 消费组详情失败 [group_id=%s]: %s", group_id, exc)
            return {"error": f"查询消费组详情失败: {exc}"}

    # 7. 注册写操作 Tools (双重安全防线之一：只读模式隐藏拦截)
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

        @server.tool(
            name="kafka_produce_message",
            description="向指定 Kafka 主题发送消息（支持 Key、JSON/文本载荷与 Headers；只读模式下不可用）",
        )
        async def kafka_produce_message(
            topic: str,
            value: Any,
            key: str | None = None,
            partition: int | None = None,
            headers: dict[str, str] | None = None,
        ) -> dict[str, Any]:
            """向指定主题发送业务消息.

            @param topic 目标主题名称
            @param value 消息载荷内容 (支持字典、列表、字符串)
            @param key 可选消息键
            @param partition 可选指定目标分区编号
            @param headers 可选自定义标头字典
            @return 写入确认元数据字典；失败时返回错误描述字典
            """
            try:
                res = await mgr.produce_message(
                    topic=topic,
                    value=value,
                    key=key,
                    partition=partition,
                    headers=headers,
                )
                return res.model_dump()
            except Exception as exc:  # noqa: BLE001
                logger.warning("生产 Kafka 消息失败 [topic=%s]: %s", topic, exc)
                return {"error": f"发送消息失败: {exc}"}


    # 8. 注册集群摘要 Resource
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

    # 9. 注册指定主题动态 Resource
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

    # 10. 注册消费组积压与故障诊断 Prompt
    @server.prompt(
        name="diagnose_topic_lag",
        description="引导大模型对 Kafka 消费组滞后 (Lag) 进行深度根因分析与拓扑倾斜诊断",
    )
    def diagnose_topic_lag(group_id: str, topic: str | None = None) -> str:
        """生成消费组积压与分区倾斜诊断引导提示词.

        @param group_id 待诊断的 Kafka 消费组 ID
        @param topic 可选的关注主题名称
        @return 结构化诊断引导提示词
        """
        topic_clause = f"针对主题 `{topic}` " if topic else ""
        topic_check = (
            f"- 若需进一步验证主题分区副本健康度，调用 `kafka_describe_topic(topic_name='{topic}')` 检查 ISR 副本及 Leader 状态。"
            if topic
            else "- 若发现特定主题积压严重，调用 `kafka_describe_topic` 检查对应主题的分区副本健康度与 Leader 分布。"
        )
        return (
            f"请对 Kafka 消费组 `{group_id}` {topic_clause}开展数据消费积压 (Lag) 与稳定性根因诊断。\n\n"
            f"## 诊断排查步骤建议\n"
            f"1. **调用工具获取最新现场数据**：\n"
            f"   - 首先调用 `kafka_describe_consumer_group(group_id='{group_id}')` 获取当前消费组的最新状态、活跃成员列表、各分区已提交位移 (Committed Offset) 与日志末端位移 (Log End Offset)。\n"
            f"   {topic_check}\n\n"
            f"2. **多维根因分析与指标评估**：\n"
            f"   - **积压总量与趋势**：评估 `total_lag` 规模，判断积压是否处于正常波动范围还是业务堆积失控。\n"
            f"   - **分区倾斜诊断 (Partition Skew)**：对比各分区 Lag 数值。若个别分区 Lag 极高而其他分区接近 0，重点排查是否存在消息 Key 热点集中、特定分区消息体过大或消费端个别 Worker 线程卡死。\n"
            f"   - **消费者活跃度与拓扑映射 (Consumer Dead / Starvation)**：\n"
            f"     - 检查消费组状态是否为 `Stable`；若处于 `Empty` 或 `PreparingRebalance`/`CompletingRebalance`，排查客户端崩溃或网络抖动。\n"
            f"     - 检查是否存在未分配给任何活跃成员的分区 (Member ID 为空)，判断是否发生成员下线或消费者数量少于分区数。\n"
            f"   - **处理耗时与心跳超时 (Processing Delay & Rebalance)**：\n"
            f"     - 结合位移差距推断业务处理速率；若客户端频繁重平衡，重点排查单批消息处理耗时是否超过 `max.poll.interval.ms`。\n\n"
            f"3. **输出排查报告与治理建议**：\n"
            f"   - **现状摘要**：消费组状态、总 Lag、涉及主题与分区数。\n"
            f"   - **风险等级**：健康 (正常) / 预警 (倾斜或轻微积压) / 严重 (消费者宕机或海量积压)。\n"
            f"   - **核心疑点与根因推断**：列出最可能的故障点。\n"
            f"   - **行动建议**：给出具体处置方案（如横向扩容消费者实例、按 Key 散列重分区、调优 `max.poll.records`、排查下游数据库慢查询等）。"
        )

    return server


def run_server(config: KafkaConfig, server: MCPServer | None = None) -> None:
    """依据配置启动 MCP 服务并根据传输协议进行调度分流.

    @param config 服务运行时配置
    @param server 可选传入预构建的 MCPServer 实例，为 None 时自动构建
    """
    logging.basicConfig(level=getattr(logging, config.log_level, logging.INFO))
    srv = server or create_mcp_server(config)

    if config.transport == "sse":
        logger.info(
            "启动 Kafka MCP 服务 (HTTP SSE 网关模式): 监听 http://%s:%d",
            config.host,
            config.port,
        )
        srv.run(transport="sse", host=config.host, port=config.port)
    else:
        logger.info("启动 Kafka MCP 服务 (标准 Stdio 管道模式)")
        srv.run(transport="stdio")


def main() -> None:
    """服务主函数 CLI 运行入口."""
    config = parse_cli_args(sys.argv[1:])
    run_server(config)


if __name__ == "__main__":
    main()

