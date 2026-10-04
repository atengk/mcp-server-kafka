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
