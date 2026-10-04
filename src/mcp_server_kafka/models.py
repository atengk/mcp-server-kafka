"""Kafka MCP 领域数据模型定义.

@author Ateng
@since 2026-10-04
"""

from typing import Any

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


class ProduceResult(BaseModel):
    """Kafka 消息发送确认元数据实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    topic: str = Field(description="目标主题名称")
    partition: int = Field(description="写入的目标分区编号")
    offset: int = Field(description="写入成功分配的位移编号")
    timestamp: int | None = Field(default=None, description="消息写入时间戳 (毫秒)")
    key: str | None = Field(default=None, description="消息键")


class SampledMessage(BaseModel):
    """采样读取的 Kafka 消息实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    partition: int = Field(description="消息所在物理分区编号")
    offset: int = Field(description="消息物理位移编号")
    timestamp: int | None = Field(default=None, description="消息时间戳 (毫秒)")
    key: str | None = Field(default=None, description="消息键 (UTF-8 文本或 Base64)")
    value: Any = Field(default=None, description="反序列化后的消息载荷 (JSON 对象/文本/Base64)")
    headers: dict[str, str] = Field(default_factory=dict, description="消息标头键值对")
    size: int = Field(default=0, description="原始载荷字节大小")
    encoding: str = Field(default="text", description="载荷编码识别类型 (json / text / base64 / null)")

