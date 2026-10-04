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
    truncated: bool = Field(default=False, description="消息体是否因超过安全阈值而被截断")
    original_size_bytes: int | None = Field(default=None, description="原始未截断消息载荷的完整字节数")


class ConsumerGroupSummary(BaseModel):
    """Kafka 消费组列表摘要实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    group_id: str = Field(description="消费组唯一标识符 ID")
    protocol_type: str = Field(default="", description="协议类型 (例如 consumer)")
    state: str = Field(default="Unknown", description="消费组状态 (例如 Stable, Empty, Dead 等)")


class ConsumerGroupMember(BaseModel):
    """Kafka 消费组活跃成员实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    member_id: str = Field(description="消费者成员唯一标识符")
    client_id: str = Field(description="客户端 ID")
    client_host: str = Field(description="客户端主机 IP 或主机名")
    partitions: list[dict[str, Any]] = Field(default_factory=list, description="分配给该成员的主题与分区列表")


class PartitionLag(BaseModel):
    """Kafka 主题分区位移与积压实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    topic: str = Field(description="主题名称")
    partition: int = Field(description="物理分区编号")
    committed_offset: int | None = Field(default=None, description="消费组已提交位移")
    log_end_offset: int | None = Field(default=None, description="分区最新日志末端位移 (LEO)")
    lag: int | None = Field(default=None, description="当前分区积压消息条数")
    member_id: str | None = Field(default=None, description="当前负责消费该分区的活跃成员 ID")
    topic_deleted: bool = Field(default=False, description="主题是否已在集群中删除")


class ConsumerGroupDetail(BaseModel):
    """Kafka 消费组拓扑详情与积压实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    group_id: str = Field(description="消费组唯一标识符 ID")
    state: str = Field(default="Unknown", description="消费组当前状态")
    protocol_type: str = Field(default="", description="协议类型")
    protocol: str = Field(default="", description="分区分配策略 (例如 range, roundrobin)")
    members: list[ConsumerGroupMember] = Field(default_factory=list, description="当前活跃成员列表")
    partitions: list[PartitionLag] = Field(default_factory=list, description="各主题分区位移与积压分布明细")
    total_lag: int = Field(default=0, description="消费组在全部有效分区上的总积压消息条数")


class KafkaConnectionSummary(BaseModel):
    """Kafka 集群连接摘要实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(description="集群连接逻辑别名")
    bootstrap_servers: str = Field(description="Broker 引导连接地址列表")
    read_only: bool = Field(description="是否处于只读保护状态")
    is_default: bool = Field(default=False, description="是否为系统全局默认回退连接")
    security_protocol: str = Field(default="PLAINTEXT", description="安全通信协议")
    sasl_mechanism: str | None = Field(default=None, description="SASL 认证算法机制")


class PartitionOffsetResetDetail(BaseModel):
    """单分区位移重置计算明细实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    partition: int = Field(description="物理分区编号")
    current_offset: int = Field(description="重置前已提交位移 (未提交时为 0)")
    target_offset: int = Field(description="目标重置位移")
    offset_delta: int = Field(description="位移变动差值 (target - current，负数表示回退，正数表示跳过)")


class OffsetResetResult(BaseModel):
    """消费组位移重置执行与预检结果实体.

    @author Ateng
    @since 2026-10-04
    """

    model_config = ConfigDict(frozen=True)

    group_id: str = Field(description="目标消费组 ID")
    topic: str = Field(description="目标主题")
    strategy: str = Field(description="所采用的重置策略 (earliest, latest, to_offset, to_datetime)")
    dry_run: bool = Field(description="是否仅为预检评估试运行")
    applied: bool = Field(description="位移变更是否已真正落盘生效")
    warning: str | None = Field(default=None, description="高危状态警示说明 (如消费组处于活跃状态)")
    partitions: list[PartitionOffsetResetDetail] = Field(default_factory=list, description="各分区位移调整明细")
    total_partitions: int = Field(default=0, description="受影响分区总数")
    total_delta: int = Field(default=0, description="总净位移变动条数")



