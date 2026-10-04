"""Kafka 客户端与 AdminClient 生命周期连接管理器.

@author Ateng
@since 2026-10-04
"""

import asyncio
import base64
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.coordinator.protocol import ConsumerProtocolMemberAssignment

from mcp_server_kafka.config import KafkaConfig
from mcp_server_kafka.models import (
    BrokerInfo,
    ClusterInfo,
    ConsumerGroupDetail,
    ConsumerGroupMember,
    ConsumerGroupSummary,
    PartitionInfo,
    PartitionLag,
    ProduceResult,
    SampledMessage,
    TopicDetail,
    TopicSummary,
)

logger = logging.getLogger(__name__)


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


def _safe_decode_bytes(raw_bytes: bytes) -> str:
    """安全解码字节流为 UTF-8 文本，失败时自动降级为 Base64 编码字符串.

    @param raw_bytes 待解码字节流
    @return 解码后的字符串
    """
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return base64.b64encode(raw_bytes).decode("ascii")


def decode_payload(raw_bytes: bytes | None) -> tuple[Any, str, int]:
    """自适应反序列化原始消息载荷字节流.

    优先尝试解析 UTF-8 JSON 结构化数据；若非合法 JSON 则保留为纯文本；
    若包含无法解码的二进制字节流，则安全降级为 Base64 字符串.

    @param raw_bytes 消息原始载荷字节流
    @return (解码后对象或文本, 识别的编码类型, 原始字节大小)
    """
    if raw_bytes is None:
        return None, "null", 0

    size = len(raw_bytes)
    try:
        text = raw_bytes.decode("utf-8")
        try:
            parsed_json = json.loads(text)
            return parsed_json, "json", size
        except (json.JSONDecodeError, ValueError):
            return text, "text", size
    except UnicodeDecodeError:
        b64_str = base64.b64encode(raw_bytes).decode("ascii")
        return b64_str, "base64", size



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
        self._producer: AIOKafkaProducer | None = None
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

    async def get_producer(self) -> AIOKafkaProducer:
        """获取或惰性初始化 AIOKafkaProducer 实例.

        @return 已启动的 AIOKafkaProducer 实例
        """
        if self._producer is not None:
            return self._producer

        async with self._lock:
            if self._producer is None:
                prod = AIOKafkaProducer(
                    bootstrap_servers=self._config.bootstrap_servers,
                )
                await prod.start()
                self._producer = prod
        return self._producer

    async def close(self) -> None:
        """关闭所有底层活跃的 Kafka 客户端连接."""
        async with self._lock:
            if self._admin_client is not None:
                await self._admin_client.close()
                self._admin_client = None
            if self._producer is not None:
                await self._producer.stop()
                self._producer = None

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

    async def produce_message(
        self,
        topic: str,
        value: Any,
        key: str | None = None,
        partition: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> ProduceResult:
        """向指定 Kafka 主题发送消息.

        @param topic 目标主题名称
        @param value 消息载荷 (支持字符串、字典、列表)
        @param key 可选消息键
        @param partition 可选指定分区号
        @param headers 可选自定义标头字典
        @return 发送确认元数据实体
        """
        # 1. 自适应编码消息载荷
        if isinstance(value, (dict, list)):
            value_bytes = json.dumps(value, ensure_ascii=False).encode("utf-8")
        elif isinstance(value, str):
            value_bytes = value.encode("utf-8")
        elif isinstance(value, bytes):
            value_bytes = value
        else:
            value_bytes = str(value).encode("utf-8")

        key_bytes = key.encode("utf-8") if isinstance(key, str) else None

        # 2. 格式化标头列表
        headers_list = (
            [(k, v.encode("utf-8") if isinstance(v, str) else v) for k, v in headers.items()]
            if headers
            else None
        )

        # 3. 发送并等待 Broker 确认
        producer = await self.get_producer()
        record_meta = await producer.send_and_wait(
            topic=topic,
            value=value_bytes,
            key=key_bytes,
            partition=partition,
            headers=headers_list,
        )

        return ProduceResult(
            topic=record_meta.topic,
            partition=record_meta.partition,
            offset=record_meta.offset,
            timestamp=record_meta.timestamp,
            key=key,
        )

    @staticmethod
    def build_sampled_message(
        partition: int,
        offset: int,
        timestamp: int | None,
        key: bytes | str | None,
        raw_value: bytes | None,
        headers: dict[str, str] | list[tuple[str, bytes]] | None = None,
    ) -> SampledMessage:
        """构建自适应反序列化后的采样消息实体.

        @param partition 分区编号
        @param offset 消息位移
        @param timestamp 时间戳
        @param key 消息键
        @param raw_value 原始消息载荷字节
        @param headers 消息标头
        @return 结构化采样消息实体
        """
        # 1. 解码消息键
        key_str: str | None = None
        if key is not None:
            key_str = key if isinstance(key, str) else _safe_decode_bytes(key)

        # 2. 自适应解码载荷
        decoded_val, encoding, size = decode_payload(raw_value)

        # 3. 整理标头键值对
        headers_dict: dict[str, str] = {}
        if isinstance(headers, dict):
            headers_dict = headers
        elif isinstance(headers, (list, tuple)):
            for item in headers:
                if len(item) >= 2:
                    k, v = item[0], item[1]
                    v_str = _safe_decode_bytes(v) if isinstance(v, bytes) else str(v)
                    headers_dict[str(k)] = v_str

        return SampledMessage(
            partition=partition,
            offset=offset,
            timestamp=timestamp,
            key=key_str,
            value=decoded_val,
            headers=headers_dict,
            size=size,
            encoding=encoding,
        )

    def create_consumer(self) -> AIOKafkaConsumer:
        """创建瞬态采样消费者实例 (强制关闭自动提交且无消费组).

        @return 配置好的 AIOKafkaConsumer 实例
        """
        return AIOKafkaConsumer(
            bootstrap_servers=self._config.bootstrap_servers,
            enable_auto_commit=False,
            group_id=None,
        )

    async def sample_messages(
        self,
        topic: str,
        partition: int | None = None,
        strategy: str = "latest",
        offset: int | None = None,
        limit: int = 10,
    ) -> list[SampledMessage]:
        """以零提交位移瞬态方式采样拉取主题消息.

        @param topic 目标主题名称
        @param partition 可选指定分区号
        @param strategy 采样策略 (latest / earliest / offset)
        @param offset 策略为 offset 时的起始数值
        @param limit 采样条数限制 (上限 100)
        @return 采样消息列表
        """
        effective_limit = min(max(limit, 1), 100)
        consumer = self.create_consumer()
        await consumer.start()
        try:
            # 1. 确定并分配分区
            if partition is not None:
                assigned_partitions = [TopicPartition(topic, partition)]
            else:
                topic_detail = await self.describe_topic(topic)
                assigned_partitions = [
                    TopicPartition(topic, p.partition_id) for p in topic_detail.partitions
                ]

            if not assigned_partitions:
                return []

            consumer.assign(assigned_partitions)

            # 2. 依照策略定位起始位移
            if strategy == "earliest":
                await consumer.seek_to_beginning(*assigned_partitions)
            elif strategy == "offset" and offset is not None:
                for tp in assigned_partitions:
                    consumer.seek(tp, offset)
            else:
                end_offsets = await consumer.end_offsets(assigned_partitions)
                for tp in assigned_partitions:
                    end_off = end_offsets.get(tp, 0)
                    start_off = max(0, end_off - effective_limit)
                    consumer.seek(tp, start_off)

            # 3. 瞬态拉取并解码
            raw_batches = await consumer.getmany(
                *assigned_partitions,
                timeout_ms=3000,
                max_records=effective_limit,
            )
            sampled: list[SampledMessage] = []
            for tp_records in raw_batches.values():
                for record in tp_records:
                    sampled.append(
                        self.build_sampled_message(
                            partition=record.partition,
                            offset=record.offset,
                            timestamp=record.timestamp,
                            key=record.key,
                            raw_value=record.value,
                            headers=record.headers,
                        )
                    )
                    if len(sampled) >= effective_limit:
                        break
                if len(sampled) >= effective_limit:
                    break

            return sampled
        finally:
            await consumer.stop()

    async def list_consumer_groups(self) -> list[ConsumerGroupSummary]:
        """列出集群所有消费组摘要信息.

        @return 消费组摘要模型列表
        """
        # 1. 查询集群中所有消费组基础清单
        admin = await self.get_admin_client()
        raw_groups = await admin.list_consumer_groups()
        if not raw_groups:
            return []

        group_ids: list[str] = []
        parsed_groups: list[tuple[str, str]] = []
        for item in raw_groups:
            if isinstance(item, (tuple, list)):
                gid = str(item[0]) if len(item) > 0 else ""
                proto = str(item[1]) if len(item) > 1 else ""
            elif isinstance(item, dict):
                gid = str(item.get("group_id") or item.get("group") or "")
                proto = str(item.get("protocol_type", ""))
            else:
                gid = str(getattr(item, "group_id", getattr(item, "group", "")))
                proto = str(getattr(item, "protocol_type", ""))
            if gid:
                group_ids.append(gid)
                parsed_groups.append((gid, proto))

        if not group_ids:
            return []

        # 2. 批量获取消费组当前状态 (如 Stable, Empty, Dead)
        state_map: dict[str, str] = {}
        try:
            desc_responses = await admin.describe_consumer_groups(group_ids)
            for resp in desc_responses:
                groups_list = (
                    resp.groups
                    if hasattr(resp, "groups")
                    else (resp.get("groups", []) if isinstance(resp, dict) else [])
                )
                for g in groups_list:
                    if isinstance(g, (tuple, list)) and len(g) >= 3:
                        err, g_name, st = g[0], g[1], g[2]
                        if err == 0:
                            state_map[str(g_name)] = str(st)
                    elif isinstance(g, dict):
                        if g.get("error_code", 0) == 0 and "group" in g:
                            state_map[str(g["group"])] = str(g.get("state", "Unknown"))
                    elif hasattr(g, "group") and getattr(g, "error_code", 0) == 0:
                        state_map[str(g.group)] = str(getattr(g, "state", "Unknown"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("批量查询消费组状态异常，回退默认状态: %s", exc)

        # 3. 构造并返回消费组摘要实体列表
        summaries: list[ConsumerGroupSummary] = []
        for gid, proto in parsed_groups:
            st = state_map.get(gid, "Unknown")
            summaries.append(ConsumerGroupSummary(group_id=gid, protocol_type=proto, state=st))
        return summaries

    async def describe_consumer_group(self, group_id: str) -> ConsumerGroupDetail:
        """查询指定消费组详情，包括活跃成员分配与各分区 Lag 积压.

        @param group_id 目标消费组唯一标识 ID
        @return 结构化消费组详情模型
        @throws ValueError 当消费组不存在或查询返回错误码时抛出
        """
        # 1. 查询消费组状态与成员信息
        admin = await self.get_admin_client()
        desc_responses = await admin.describe_consumer_groups([group_id])

        target_group = None
        for resp in desc_responses:
            groups_list = (
                resp.groups
                if hasattr(resp, "groups")
                else (resp.get("groups", []) if isinstance(resp, dict) else [])
            )
            for g in groups_list:
                g_id = (
                    g[1]
                    if isinstance(g, (tuple, list)) and len(g) > 1
                    else (g.get("group") if isinstance(g, dict) else getattr(g, "group", None))
                )
                if g_id == group_id:
                    target_group = g
                    break
            if target_group is not None:
                break

        if target_group is None:
            raise ValueError(f"消费组 '{group_id}' 不存在或查询失败")

        if isinstance(target_group, (tuple, list)):
            err_code = target_group[0] if len(target_group) > 0 else 0
            state = str(target_group[2]) if len(target_group) > 2 else "Unknown"
            protocol_type = str(target_group[3]) if len(target_group) > 3 else ""
            protocol = str(target_group[4]) if len(target_group) > 4 else ""
            raw_members = target_group[5] if len(target_group) > 5 else []
        elif isinstance(target_group, dict):
            err_code = target_group.get("error_code", 0)
            state = str(target_group.get("state", "Unknown"))
            protocol_type = str(target_group.get("protocol_type", ""))
            protocol = str(target_group.get("protocol", ""))
            raw_members = target_group.get("members", [])
        else:
            err_code = getattr(target_group, "error_code", 0)
            state = str(getattr(target_group, "state", "Unknown"))
            protocol_type = str(getattr(target_group, "protocol_type", ""))
            protocol = str(getattr(target_group, "protocol", ""))
            raw_members = getattr(target_group, "members", [])

        if err_code != 0:
            raise ValueError(f"消费组 '{group_id}' 查询返回错误码: {err_code}")

        # 2. 解析活跃成员与分区分配关系
        member_entities: list[ConsumerGroupMember] = []
        member_by_tp: dict[TopicPartition, str] = {}
        assigned_tps_from_members: set[TopicPartition] = set()

        for m in raw_members:
            if isinstance(m, (tuple, list)):
                mid = str(m[0]) if len(m) > 0 else ""
                cid = str(m[1]) if len(m) > 1 else ""
                chost = str(m[2]) if len(m) > 2 else ""
                massign = m[4] if len(m) > 4 else b""
            elif isinstance(m, dict):
                mid = str(m.get("member_id", ""))
                cid = str(m.get("client_id", ""))
                chost = str(m.get("client_host", ""))
                massign = m.get("member_assignment", b"")
            else:
                mid = str(getattr(m, "member_id", ""))
                cid = str(getattr(m, "client_id", ""))
                chost = str(getattr(m, "client_host", ""))
                massign = getattr(m, "member_assignment", b"")

            m_partitions: list[dict[str, Any]] = []
            if massign and isinstance(massign, (bytes, bytearray)):
                try:
                    decoded_assign = ConsumerProtocolMemberAssignment.decode(massign)
                    for tp in decoded_assign.partitions():
                        m_partitions.append({"topic": tp.topic, "partition": tp.partition})
                        member_by_tp[tp] = mid
                        assigned_tps_from_members.add(tp)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("解码成员 [%s] 分区分配字节流失败: %s", mid, exc)
            elif isinstance(massign, list):
                for item in massign:
                    if isinstance(item, TopicPartition):
                        m_partitions.append({"topic": item.topic, "partition": item.partition})
                        member_by_tp[item] = mid
                        assigned_tps_from_members.add(item)
                    elif isinstance(item, dict):
                        tp_obj = TopicPartition(item["topic"], int(item["partition"]))
                        m_partitions.append({"topic": tp_obj.topic, "partition": tp_obj.partition})
                        member_by_tp[tp_obj] = mid
                        assigned_tps_from_members.add(tp_obj)

            member_entities.append(
                ConsumerGroupMember(
                    member_id=mid,
                    client_id=cid,
                    client_host=chost,
                    partitions=m_partitions,
                )
            )

        # 3. 查询消费组已提交位移
        committed_offsets_map: dict[TopicPartition, int | None] = {}
        try:
            offsets_resp = await admin.list_consumer_group_offsets(group_id)
            for tp, offset_meta in offsets_resp.items():
                if offset_meta is not None and getattr(offset_meta, "offset", -1) >= 0:
                    committed_offsets_map[tp] = offset_meta.offset
                else:
                    committed_offsets_map[tp] = None
        except Exception as exc:  # noqa: BLE001
            logger.warning("查询消费组 [%s] 提交位移失败: %s", group_id, exc)

        # 4. 汇总全量涉及的分区并进行现存主题防御过滤
        all_tps: set[TopicPartition] = set(committed_offsets_map.keys()) | assigned_tps_from_members
        existing_topics = await self.list_topics(include_internal=True)
        existing_topic_names = {t.name for t in existing_topics}

        # 补充涉足主题的所有物理分区（覆盖尚未分配或尚未提交位移的新分区）
        involved_topics = {tp.topic for tp in all_tps if tp.topic in existing_topic_names}
        for top_name in involved_topics:
            try:
                t_detail = await self.describe_topic(top_name)
                for part in t_detail.partitions:
                    all_tps.add(TopicPartition(top_name, part.partition_id))
            except Exception as exc:  # noqa: BLE001
                logger.debug("获取主题 [%s] 物理分区扩展信息失败: %s", top_name, exc)

        if not all_tps:
            return ConsumerGroupDetail(
                group_id=group_id,
                state=state,
                protocol_type=protocol_type,
                protocol=protocol,
                members=member_entities,
                partitions=[],
                total_lag=0,
            )

        # 5. 查询现存物理分区的日志末端位移 (LEO)
        valid_tps = [tp for tp in all_tps if tp.topic in existing_topic_names]
        deleted_tps = {tp for tp in all_tps if tp.topic not in existing_topic_names}

        log_end_offsets_map: dict[TopicPartition, int] = {}
        if valid_tps:
            consumer = self.create_consumer()
            await consumer.start()
            try:
                log_end_offsets_map = await consumer.end_offsets(valid_tps)
            finally:
                await consumer.stop()

        # 6. 计算各分区精确 Lag 与汇总 Total Lag
        partition_lags: list[PartitionLag] = []
        total_lag: int = 0

        for tp in sorted(all_tps, key=lambda x: (x.topic, x.partition)):
            is_deleted = tp in deleted_tps
            committed = committed_offsets_map.get(tp)
            mid = member_by_tp.get(tp)

            if is_deleted:
                partition_lags.append(
                    PartitionLag(
                        topic=tp.topic,
                        partition=tp.partition,
                        committed_offset=committed,
                        log_end_offset=None,
                        lag=None,
                        member_id=mid,
                        topic_deleted=True,
                    )
                )
            else:
                end_offset = log_end_offsets_map.get(tp)
                if end_offset is not None:
                    if committed is not None and committed >= 0:
                        calc_lag = max(0, end_offset - committed)
                    else:
                        calc_lag = end_offset
                    total_lag += calc_lag
                else:
                    calc_lag = None

                partition_lags.append(
                    PartitionLag(
                        topic=tp.topic,
                        partition=tp.partition,
                        committed_offset=committed,
                        log_end_offset=end_offset,
                        lag=calc_lag,
                        member_id=mid,
                        topic_deleted=False,
                    )
                )

        return ConsumerGroupDetail(
            group_id=group_id,
            state=state,
            protocol_type=protocol_type,
            protocol=protocol,
            members=member_entities,
            partitions=partition_lags,
            total_lag=total_lag,
        )


