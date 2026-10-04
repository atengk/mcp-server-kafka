"""Kafka MCP 服务启动入口与协议路由装配.

@author Ateng
@since 2026-10-04
"""

import logging
import sys
from typing import Any

from mcp.server.mcpserver import MCPServer

from mcp_server_kafka.config import KafkaConfig, parse_cli_args
from mcp_server_kafka.manager import KafkaManager, KafkaManagerRegistry

logger = logging.getLogger(__name__)


def create_mcp_server(
    config: KafkaConfig | None = None,
    manager: KafkaManager | None = None,
    registry: KafkaManagerRegistry | None = None,
) -> MCPServer:
    """构建并装配 Kafka MCP 服务端实例.

    @param config 服务运行时配置，默认从环境变量或预设缺省值加载
    @param manager 单个 Kafka 连接管理器实例，传入时自动挂载为默认连接
    @param registry 完整的多集群连接注册中心实例，优先使用
    @return 已装配 Tools 与 Resources 的 MCPServer 实例
    """
    cfg = config or KafkaConfig.from_env()
    if registry is not None:
        reg = registry
    elif manager is not None:
        reg = KafkaManagerRegistry(cfg)
        reg._managers[cfg.default_connection] = manager
    else:
        reg = KafkaManagerRegistry(cfg)

    server = MCPServer(
        name="atengk-mcp-server-kafka",
        instructions="Apache Kafka 模型上下文协议 (MCP) 服务端，支持多集群管理、元数据探查、零位移采样与积压诊断.",
    )

    # 1. 注册集群连接清单查询 Tool
    @server.tool(
        name="kafka_list_connections",
        description="枚举当前服务端已配置的所有 Kafka 集群命名连接清单、Broker 引导地址与只读保护状态",
    )
    async def kafka_list_connections() -> dict[str, Any]:
        """枚举所有已配置的 Kafka 集群连接.

        @return 包含 connections 列表与 default_connection 标识的字典
        """
        connections = reg.list_connections()
        return {
            "default_connection": reg.default_connection_name,
            "connections": [c.model_dump() for c in connections],
            "count": len(connections),
        }

    # 2. 注册集群元数据查询 Tool
    @server.tool(
        name="kafka_cluster_info",
        description="获取 Kafka 集群整体元数据与 Broker 节点列表（包含 Cluster ID、控制器与节点地址）",
    )
    async def kafka_cluster_info(connection: str | None = None) -> dict[str, Any]:
        """获取 Kafka 集群元数据与 Broker 节点信息.

        @param connection 可选集群连接别名，未传或为空时使用默认连接
        @return 包含集群标识符、控制器及节点列表的元数据字典；失败时返回错误描述字典
        """
        try:
            mgr = reg.get_manager(connection)
            cluster = await mgr.get_cluster_metadata()
            return cluster.model_dump()
        except Exception as exc:  # noqa: BLE001
            logger.warning("获取 Kafka 集群元数据失败 [connection=%s]: %s", connection, exc)
            return {"error": f"获取集群元数据失败: {exc}"}

    # 3. 注册主题列表查询 Tool
    @server.tool(
        name="kafka_list_topics",
        description="列出 Kafka 集群中的主题清单（支持按名称模式过滤并默认排除内部系统主题）",
    )
    async def kafka_list_topics(
        pattern: str | None = None,
        include_internal: bool = False,
        connection: str | None = None,
    ) -> dict[str, Any]:
        """列出 Kafka 集群中的主题摘要信息.

        @param pattern 可选名称模式过滤字符串（模糊匹配）
        @param include_internal 是否包含系统内部主题 (如 __consumer_offsets)
        @param connection 可选集群连接别名，未传或为空时使用默认连接
        @return 包含主题清单 topics 与总数 count 的结果字典；失败时返回错误描述字典
        """
        try:
            mgr = reg.get_manager(connection)
            summaries = await mgr.list_topics(pattern=pattern, include_internal=include_internal)
            return {
                "connection": connection or reg.default_connection_name,
                "topics": [s.model_dump() for s in summaries],
                "count": len(summaries),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("获取 Kafka 主题列表失败 [connection=%s]: %s", connection, exc)
            return {"error": f"获取主题列表失败: {exc}"}

    # 4. 注册主题详细信息查询 Tool
    @server.tool(
        name="kafka_describe_topic",
        description="查询指定 Kafka 主题的详细拓扑（分区分布、Leader 节点、ISR 同步副本与配置）",
    )
    async def kafka_describe_topic(
        topic_name: str,
        connection: str | None = None,
    ) -> dict[str, Any]:
        """查询指定 Kafka 主题详细信息.

        @param topic_name 目标主题名称
        @param connection 可选集群连接别名，未传或为空时使用默认连接
        @return 包含分区列表与自定义配置的主题详情字典
        """
        try:
            mgr = reg.get_manager(connection)
            detail = await mgr.describe_topic(topic_name=topic_name)
            return detail.model_dump()
        except Exception as exc:  # noqa: BLE001
            logger.warning("查询 Kafka 主题详情失败 [topic=%s, connection=%s]: %s", topic_name, connection, exc)
            return {"error": f"查询主题详情失败: {exc}"}

    # 5. 注册消息采样 Tool (零提交位移只读探查)
    @server.tool(
        name="kafka_sample_messages",
        description="从指定 Kafka 主题以零提交位移方式只读采样消息（支持 latest/earliest/offset 策略、自适应解码与大报文截断保护）",
    )
    async def kafka_sample_messages(
        topic: str,
        partition: int | None = None,
        strategy: str = "latest",
        offset: int | None = None,
        limit: int = 10,
        max_bytes_per_message: int = 65536,
        connection: str | None = None,
    ) -> dict[str, Any]:
        """以零位移影响方式采样拉取主题消息.

        @param topic 目标主题名称
        @param partition 可选指定物理分区
        @param strategy 采样策略 (latest / earliest / offset)，默认 latest
        @param offset 当 strategy=offset 时的起始数值
        @param limit 采样条数限制，默认 10，上限 100
        @param max_bytes_per_message 单条消息最大载荷字节数，超出时实施截断，默认 65536 (64KB)
        @param connection 可选集群连接别名，未传或为空时使用默认连接
        @return 包含 messages 列表与 total 总数的结果字典；失败时返回错误描述字典
        """
        try:
            mgr = reg.get_manager(connection)
            sampled = await mgr.sample_messages(
                topic=topic,
                partition=partition,
                strategy=strategy,
                offset=offset,
                limit=limit,
                max_bytes_per_message=max_bytes_per_message,
            )
            return {
                "connection": connection or reg.default_connection_name,
                "topic": topic,
                "messages": [m.model_dump() for m in sampled],
                "total": len(sampled),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("采样 Kafka 消息失败 [topic=%s, connection=%s]: %s", topic, connection, exc)
            return {"error": f"采样消息失败: {exc}"}


    # 6. 注册消费组列表查询 Tool
    @server.tool(
        name="kafka_list_consumer_groups",
        description="列出 Kafka 集群中的消费组清单（包含 Group ID、协议类型与活跃状态如 Stable/Empty/Dead）",
    )
    async def kafka_list_consumer_groups(connection: str | None = None) -> dict[str, Any]:
        """列出 Kafka 集群中的消费组摘要信息.

        @param connection 可选集群连接别名，未传或为空时使用默认连接
        @return 包含消费组清单 groups 与总数 count 的结果字典；失败时返回错误描述字典
        """
        try:
            mgr = reg.get_manager(connection)
            groups = await mgr.list_consumer_groups()
            return {
                "connection": connection or reg.default_connection_name,
                "groups": [g.model_dump() for g in groups],
                "count": len(groups),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("获取 Kafka 消费组列表失败 [connection=%s]: %s", connection, exc)
            return {"error": f"获取消费组列表失败: {exc}"}

    # 7. 注册消费组详情与积压分析 Tool
    @server.tool(
        name="kafka_describe_consumer_group",
        description="查询指定 Kafka 消费组的详细拓扑，包含活跃成员、分区 Committed Offset、LEO 及 Lag 积压数值",
    )
    async def kafka_describe_consumer_group(
        group_id: str,
        connection: str | None = None,
    ) -> dict[str, Any]:
        """查询指定 Kafka 消费组详细拓扑与分区积压.

        @param group_id 目标消费组 ID
        @param connection 可选集群连接别名，未传或为空时使用默认连接
        @return 包含活跃成员分配及各分区 Lag 积压明细的详情字典；失败时返回错误描述字典
        """
        try:
            mgr = reg.get_manager(connection)
            detail = await mgr.describe_consumer_group(group_id=group_id)
            return detail.model_dump()
        except Exception as exc:  # noqa: BLE001
            logger.warning("查询 Kafka 消费组详情失败 [group_id=%s, connection=%s]: %s", group_id, connection, exc)
            return {"error": f"查询消费组详情失败: {exc}"}

    # 8. 注册写操作 Tools (双重安全防线：全局只读隐藏 + 连接级只读精准拦截)
    if not reg.global_config.read_only:

        @server.tool(
            name="kafka_create_topic",
            description="在 Kafka 集群中创建新主题（支持指定分区数与副本因子；只读模式下不可用）",
        )
        async def kafka_create_topic(
            topic_name: str,
            partitions: int = 1,
            replication_factor: int = 1,
            connection: str | None = None,
        ) -> dict[str, Any]:
            """创建新的 Kafka 主题.

            @param topic_name 待创建的主题名称
            @param partitions 分区数量，默认 1
            @param replication_factor 副本因子，默认 1
            @param connection 可选集群连接别名，未传或为空时使用默认连接
            @return 创建状态结果描述
            """
            target_name = connection.strip() if connection and connection.strip() else reg.default_connection_name
            if reg.is_connection_read_only(target_name):
                return {"error": f"目标 Kafka 集群连接 '{target_name}' 已配置为只读保护模式，禁止执行创建主题操作"}
            try:
                mgr = reg.get_manager(connection)
                return await mgr.create_topic(
                    topic_name=topic_name,
                    partitions=partitions,
                    replication_factor=replication_factor,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("创建 Kafka 主题失败 [topic=%s, connection=%s]: %s", topic_name, connection, exc)
                return {"error": f"创建主题失败: {exc}"}

        @server.tool(
            name="kafka_delete_topic",
            description="从 Kafka 集群中删除主题（高危破坏操作，必须传入 confirm=True 显式确认）",
        )
        async def kafka_delete_topic(
            topic_name: str,
            confirm: bool = False,
            connection: str | None = None,
        ) -> dict[str, Any]:
            """删除指定的 Kafka 主题.

            @param topic_name 待删除的主题名称
            @param confirm 破坏性操作显式确认标志，必须为 True 方可执行
            @param connection 可选集群连接别名，未传或为空时使用默认连接
            @return 删除状态结果描述
            """
            target_name = connection.strip() if connection and connection.strip() else reg.default_connection_name
            if reg.is_connection_read_only(target_name):
                return {"error": f"目标 Kafka 集群连接 '{target_name}' 已配置为只读保护模式，禁止执行删除主题操作"}
            # 二级防线：破坏性动作显式二次确认校验
            if not confirm:
                return {
                    "error": "删除主题属于高危破坏性操作，必须显式传入 confirm=True 二次确认以防数据丢失",
                }
            try:
                mgr = reg.get_manager(connection)
                return await mgr.delete_topic(topic_name=topic_name)
            except Exception as exc:  # noqa: BLE001
                logger.warning("删除 Kafka 主题失败 [topic=%s, connection=%s]: %s", topic_name, connection, exc)
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
            connection: str | None = None,
        ) -> dict[str, Any]:
            """向指定主题发送业务消息.

            @param topic 目标主题名称
            @param value 消息载荷内容 (支持字典、列表、字符串)
            @param key 可选消息键
            @param partition 可选指定目标分区编号
            @param headers 可选自定义标头字典
            @param connection 可选集群连接别名，未传或为空时使用默认连接
            @return 写入确认元数据字典；失败时返回错误描述字典
            """
            target_name = connection.strip() if connection and connection.strip() else reg.default_connection_name
            if reg.is_connection_read_only(target_name):
                return {"error": f"目标 Kafka 集群连接 '{target_name}' 已配置为只读保护模式，禁止执行发送消息操作"}
            try:
                mgr = reg.get_manager(connection)
                res = await mgr.produce_message(
                    topic=topic,
                    value=value,
                    key=key,
                    partition=partition,
                    headers=headers,
                )
                return res.model_dump()
            except Exception as exc:  # noqa: BLE001
                logger.warning("生产 Kafka 消息失败 [topic=%s, connection=%s]: %s", topic, connection, exc)
                return {"error": f"发送消息失败: {exc}"}

    # 8. 注册位移重置治理 Tool (支持 Dry-Run 预检试运行，在只读模式下亦开放预检评估)
    reset_tool_desc = (
        ("【只读模式：仅支持 dry_run=True 预检评估，物理提交将被严格拦截】" if reg.global_config.read_only else "")
        + "重置指定消费组在目标主题上的消费位移（高危运维操作，支持 Dry-Run 预检与 earliest/latest/to_offset/to_datetime 策略）"
    )

    @server.tool(
        name="kafka_reset_consumer_group_offsets",
        description=reset_tool_desc,
    )
    async def kafka_reset_consumer_group_offsets(
        group_id: str,
        topic: str,
        strategy: str = "earliest",
        offset: int | None = None,
        datetime_val: str | float | None = None,
        partitions: list[int] | None = None,
        dry_run: bool = True,
        confirm: bool = False,
        force: bool = False,
        connection: str | None = None,
    ) -> dict[str, Any]:
        """重置 Kafka 消费组消费位移.

        @param group_id 目标消费组 ID
        @param topic 目标主题名称
        @param strategy 重置策略 (earliest, latest, to_offset, to_datetime)，默认 earliest
        @param offset 当 strategy=to_offset 时的目标数值
        @param datetime_val 当 strategy=to_datetime 时的 ISO 时间串或毫秒时间戳
        @param partitions 可选指定重置的分区列表，默认针对全部分区
        @param dry_run 预检模式开关，默认为 True (仅评估回显影响范围，不真正落盘)
        @param confirm 高危操作显式确认标志，真正落盘 (dry_run=False) 时强制要求为 True
        @param force 强制跳过活跃消费组冲突防御，默认 False
        @param connection 可选集群连接别名，未传或为空时使用默认连接
        @return 包含每个分区调整前后位移、差值及状态的结果字典
        """
        target_name = connection.strip() if connection and connection.strip() else reg.default_connection_name
        is_conn_read_only = reg.is_connection_read_only(target_name)

        # 只读防线：只读模式下允许 dry_run=True 评估，严格禁止物理提交落盘
        if is_conn_read_only and not dry_run:
            return {
                "error": f"目标 Kafka 集群连接 '{target_name}' 已配置为只读保护模式，禁止执行物理位移重置提交 (dry_run=False)。若需评估变动影响，请保持 dry_run=True 执行只读预检。"
            }

        # 三级防线：真正执行落盘时强制要求 confirm=True
        if not dry_run and not confirm:
            return {
                "error": "真正执行消费组位移重置属于高危变更操作，必须同时显式传入 dry_run=False 和 confirm=True 授权确认",
            }

        try:
            mgr = reg.get_manager(connection)
            res = await mgr.reset_consumer_group_offsets(
                group_id=group_id,
                topic=topic,
                strategy=strategy,
                offset=offset,
                datetime_val=datetime_val,
                partitions=partitions,
                dry_run=dry_run,
                force=force,
            )
            return res.model_dump()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "重置 Kafka 消费组位移失败 [group=%s, topic=%s, connection=%s]: %s",
                group_id,
                topic,
                connection,
                exc,
            )
            return {"error": f"重置位移失败: {exc}"}


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
            mgr_instance = reg.get_manager()
            cluster = await mgr_instance.get_cluster_metadata()
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
            mgr_instance = reg.get_manager()
            detail = await mgr_instance.describe_topic(topic_name=topic)
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

    # 11. 注册主题消息采样探查与清洗分析 Prompt
    @server.prompt(
        name="inspect_topic_messages",
        description="引导大模型对 Kafka 主题消息数据进行采样探查、载荷反序列化校验与业务数据清洗分析",
    )
    def inspect_topic_messages(
        topic: str,
        limit: int = 10,
        strategy: str = "latest",
    ) -> str:
        """生成主题消息采样探查与数据分析引导提示词.

        @param topic 待探查的 Kafka 主题名称
        @param limit 采样消息条数 (默认 10)
        @param strategy 采样策略 (latest / earliest / offset)
        @return 结构化探查引导提示词
        """
        return (
            f"请对 Kafka 主题 `{topic}` 开展消息内容只读采样与业务载荷质量分析。\n\n"
            f"## 探查步骤建议\n"
            f"1. **零位移安全采样读取**：\n"
            f"   - 调用 `kafka_sample_messages(topic='{topic}', strategy='{strategy}', limit={limit})` 瞬态拉取样本消息（该工具不接入消费组、绝不提交消费位移，不会影响下游业务生产进度）。\n\n"
            f"2. **载荷格式与元数据校验**：\n"
            f"   - **编码识别**：检查返回消息中每条消息的 `encoding` 字段（是否为期望的 `json` 或 `text`，若降级为 `base64` 排查是否存在 Avro/Protobuf 或非 UTF-8 编码）。\n"
            f"   - **键 (Key) 与标头 (Headers)**：评估业务路由键是否合理分布，标头是否包含必要的 TraceID、事件类型等链路追踪信息。\n"
            f"   - **数据结构与模式一致性 (Schema Validation)**：抽检载荷字段结构，判断关键业务字段是否存在空值或脏数据。\n\n"
            f"3. **输出探查评估报告**：\n"
            f"   - **消息概览**：分区分布、时间戳新鲜度、载荷平均大小。\n"
            f"   - **数据质量诊断**：格式规范性、模式一致性评估、异常样本归纳。\n"
            f"   - **优化建议**：针对序列化格式、消息体积、路由键设计提出优化方案。"
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

