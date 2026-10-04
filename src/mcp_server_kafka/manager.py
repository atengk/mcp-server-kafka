"""Kafka 客户端与 AdminClient 生命周期连接管理器.

@author Ateng
@since 2026-10-04
"""

import asyncio
from typing import Any

from aiokafka.admin import AIOKafkaAdminClient

from mcp_server_kafka.config import KafkaConfig
from mcp_server_kafka.models import BrokerInfo, ClusterInfo


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
