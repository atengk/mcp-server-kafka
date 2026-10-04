"""Kafka 客户端与 AdminClient 生命周期连接管理器.

@author Ateng
@since 2026-10-04
"""

import asyncio
from typing import Any

from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from mcp_server_kafka.config import KafkaConfig
from mcp_server_kafka.models import (
    BrokerInfo,
    ClusterInfo,
    PartitionInfo,
    TopicDetail,
    TopicSummary,
)


def _parse_broker_item(raw_broker: Any) -> BrokerInfo:
    """将底层多样化协议数据载荷规整为统一的 BrokerInfo 实体.

    @param raw_broker 原始节点载荷（字典、元组或对象）
    @return 结构化节点实体
    """
    if isinstance(raw_broker, dict):
        return BrokerInfo(
            node_id=int(raw_broker["node_id"]),
            host=str(raw_broker["host"]),
            port=int(raw_broker["port"]),
            rack=str(raw_broker["rack"]) if raw_broker.get("rack") else None,
        )
    if isinstance(raw_broker, (tuple, list)):
        node_id = int(raw_broker[0])
        host = str(raw_broker[1])
        port = int(raw_broker[2])
        rack = str(raw_broker[3]) if len(raw_broker) > 3 and raw_broker[3] is not None else None
        return BrokerInfo(node_id=node_id, host=host, port=port, rack=rack)
    return BrokerInfo(
        node_id=int(raw_broker.node_id),
        host=str(raw_broker.host),
        port=int(raw_broker.port),
        rack=str(raw_broker.rack) if getattr(raw_broker, "rack", None) else None,
    )


def _parse_partition_item(raw_part: Any) -> PartitionInfo:
    """将底层分区元数据规整为统一的 PartitionInfo 实体.

    @param raw_part 底层分区信息
    @return 结构化分区实体
    """
    if isinstance(raw_part, dict):
        part_id = int(raw_part.get("partition", raw_part.get("partition_id", 0)))
        leader = int(raw_part["leader"]) if raw_part.get("leader") is not None else None
        replicas = [int(r) for r in raw_part.get("replicas", [])]
        isr = [int(i) for i in raw_part.get("isr", [])]
        return PartitionInfo(partition_id=part_id, leader=leader, replicas=replicas, isr=isr)
    if isinstance(raw_part, (tuple, list)):
        # (error_code, partition, leader, replicas, isr)
        part_id = int(raw_part[1])
        leader = int(raw_part[2]) if raw_part[2] is not None and raw_part[2] >= 0 else None
        replicas = [int(r) for r in raw_part[3]] if len(raw_part) > 3 else []
        isr = [int(i) for i in raw_part[4]] if len(raw_part) > 4 else []
        return PartitionInfo(partition_id=part_id, leader=leader, replicas=replicas, isr=isr)
    return PartitionInfo(
        partition_id=int(getattr(raw_part, "partition", 0)),
        leader=getattr(raw_part, "leader", None),
        replicas=list(getattr(raw_part, "replicas", [])),
        isr=list(getattr(raw_part, "isr", [])),
    )


def _parse_topic_detail(raw_topic: Any) -> TopicDetail:
    """将底层主题元数据规整为统一的 TopicDetail 实体.

    @param raw_topic 底层主题元数据
    @return 结构化主题详情
    """
    if isinstance(raw_topic, dict):
        name = str(raw_topic["topic"])
        is_internal = bool(raw_topic.get("is_internal", False))
        parts = [_parse_partition_item(p) for p in raw_topic.get("partitions", [])]
        return TopicDetail(name=name, is_internal=is_internal, partitions=parts)
    if isinstance(raw_topic, (tuple, list)):
        # (error_code, topic, is_internal, partitions)
        name = str(raw_topic[1])
        is_internal = bool(raw_topic[2]) if len(raw_topic) > 2 else False
        raw_parts = raw_topic[3] if len(raw_topic) > 3 else []
        parts = [_parse_partition_item(p) for p in raw_parts]
        return TopicDetail(name=name, is_internal=is_internal, partitions=parts)
    return TopicDetail(
        name=str(getattr(raw_topic, "topic", "")),
        is_internal=bool(getattr(raw_topic, "is_internal", False)),
        partitions=[_parse_partition_item(p) for p in getattr(raw_topic, "partitions", [])],
    )


class KafkaManager:
    """Kafka 异步客户端与生命周期管理器.

    负责底层 AIOKafkaAdminClient 连接维护、元数据查询与资源回收.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self, config: KafkaConfig) -> None:
        """初始化管理器实例.

        @param config Kafka 服务运行时配置
        """
        self._config = config
        self._admin_client: AIOKafkaAdminClient | None = None
        self._lock = asyncio.Lock()

    @property
    def config(self) -> KafkaConfig:
        """获取当前运行时配置."""
        return self._config

    async def get_admin_client(self) -> AIOKafkaAdminClient:
        """获取或惰性初始化 AIOKafkaAdminClient 实例.

        @return 已启动的 AIOKafkaAdminClient 实例
        """
        if self._admin_client is not None:
            return self._admin_client

        async with self._lock:
            if self._admin_client is None:
                client = AIOKafkaAdminClient(
                    bootstrap_servers=self._config.bootstrap_servers,
                )
                await client.start()
                self._admin_client = client
        return self._admin_client

    async def close(self) -> None:
        """关闭所有底层活跃的 Kafka 客户端连接."""
        async with self._lock:
            if self._admin_client is not None:
                await self._admin_client.close()
                self._admin_client = None

    async def get_cluster_metadata(self) -> ClusterInfo:
        """查询并构建集群元数据与 Broker 拓扑信息.

        @return 集群拓扑元数据实体
        @throws Exception 当网络无法连接或 Broker 不可用时抛出异常
        """
        # 1. 取得活跃 AdminClient 并查询集群概况
        admin = await self.get_admin_client()
        cluster_obj: dict[str, Any] = await admin.describe_cluster()

        # 2. 提取并组装 Broker 节点列表
        raw_brokers = cluster_obj.get("brokers", [])
        brokers: list[BrokerInfo] = [_parse_broker_item(item) for item in raw_brokers]

        # 3. 解析控制器 (Controller) 节点
        controller_id = cluster_obj.get("controller_id")
        controller: BrokerInfo | None = None
        if controller_id is not None:
            for broker in brokers:
                if broker.node_id == controller_id:
                    controller = broker
                    break

        # 4. 构建并返回领域模型
        cluster_id = cluster_obj.get("cluster_id")
        return ClusterInfo(
            cluster_id=str(cluster_id) if cluster_id is not None else None,
            controller=controller,
            brokers=brokers,
        )

    async def list_topics(
        self,
        pattern: str | None = None,
        include_internal: bool = False,
    ) -> list[TopicSummary]:
        """按过滤条件列出集群中的所有主题概要信息.

        @param pattern 可选名称模式过滤字符串（模糊匹配）
        @param include_internal 是否包含系统内部主题
        @return 主题摘要列表
        """
        # 1. 获取全量主题列表并查询分区拓扑
        admin = await self.get_admin_client()
        all_topic_names: list[str] = await admin.list_topics()
        if not all_topic_names:
            return []

        raw_topics = await admin.describe_topics(all_topic_names)

        # 2. 解析为结构化模型并按条件过滤
        summaries: list[TopicSummary] = []
        for raw_topic in raw_topics:
            detail = _parse_topic_detail(raw_topic)
            if not include_internal and detail.is_internal:
                continue
            if pattern and pattern not in detail.name:
                continue
            summaries.append(
                TopicSummary(
                    name=detail.name,
                    partitions_count=len(detail.partitions),
                    is_internal=detail.is_internal,
                )
            )
        return summaries

    async def describe_topic(self, topic_name: str) -> TopicDetail:
        """查询指定主题的详细分区分布与元数据.

        @param topic_name 目标主题名称
        @return 结构化主题详情
        @throws ValueError 当主题不存在时抛出
        """
        admin = await self.get_admin_client()
        raw_topics = await admin.describe_topics([topic_name])
        if not raw_topics:
            raise ValueError(f"主题 '{topic_name}' 不存在")
        detail = _parse_topic_detail(raw_topics[0])
        if not detail.name or detail.name != topic_name:
            raise ValueError(f"主题 '{topic_name}' 不存在")
        return detail

    async def create_topic(
        self,
        topic_name: str,
        partitions: int = 1,
        replication_factor: int = 1,
    ) -> dict[str, Any]:
        """在集群中创建新的主题.

        @param topic_name 新建主题名称
        @param partitions 分区数，默认 1
        @param replication_factor 副本因子，默认 1
        @return 创建结果描述
        """
        admin = await self.get_admin_client()
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=partitions,
            replication_factor=replication_factor,
        )
        await admin.create_topics([new_topic])
        return {
            "topic": topic_name,
            "partitions": partitions,
            "replication_factor": replication_factor,
            "status": "created",
        }

    async def delete_topic(self, topic_name: str) -> dict[str, Any]:
        """从集群中删除指定的主题.

        @param topic_name 待删除的主题名称
        @return 删除结果描述
        """
        admin = await self.get_admin_client()
        await admin.delete_topics([topic_name])
        return {"topic": topic_name, "status": "deleted"}
