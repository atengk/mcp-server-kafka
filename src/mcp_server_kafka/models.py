"""Kafka MCP 领域数据模型定义.

@author Ateng
@since 2026-10-04
"""

from pydantic import BaseModel, ConfigDict, Field


class BrokerInfo(BaseModel):
    """Kafka 集群节点元数据实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    node_id: int = Field(description="节点唯一数字标识 ID")
    host: str = Field(description="节点主机名或 IP 地址")
    port: int = Field(description="节点连接端口号")
    rack: str | None = Field(default=None, description="节点机架标识")


class ClusterInfo(BaseModel):
    """Kafka 集群拓扑元数据实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    cluster_id: str | None = Field(default=None, description="集群唯一标识符")
    controller: BrokerInfo | None = Field(default=None, description="集群活跃控制器节点信息")
    brokers: list[BrokerInfo] = Field(default_factory=list, description="集群所有可用 Broker 节点列表")


class TopicSummary(BaseModel):
    """Kafka 主题列表摘要实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(description="主题名称")
    partitions_count: int = Field(description="主题拥有的物理分区数量")
    is_internal: bool = Field(default=False, description="是否为系统内部主题 (如 __consumer_offsets)")


class PartitionInfo(BaseModel):
    """Kafka 分区元数据实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    partition_id: int = Field(description="分区唯一数字索引")
    leader: int | None = Field(default=None, description="主副本 (Leader) 所在 Broker ID")
    replicas: list[int] = Field(default_factory=list, description="全量副本节点 ID 列表")
    isr: list[int] = Field(default_factory=list, description="同步就绪副本 (In-Sync Replicas) 节点 ID 列表")


class TopicDetail(BaseModel):
    """Kafka 主题详细拓扑与配置实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(description="主题名称")
    is_internal: bool = Field(default=False, description="是否为系统内部主题")
    partitions: list[PartitionInfo] = Field(default_factory=list, description="物理分区分布明细列表")
    configs: dict[str, str] = Field(default_factory=dict, description="主题自定义配置项键值对")
