"""Kafka 客户端与 AdminClient 生命周期连接管理器.

@author Ateng
@since 2026-10-04
"""

import asyncio
import base64
import json
import logging
import ssl
from datetime import datetime
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.coordinator.protocol import ConsumerProtocolMemberAssignment
from aiokafka.structs import OffsetAndMetadata

from mcp_server_kafka.config import KafkaConfig, KafkaConnectionConfig
from mcp_server_kafka.models import (
    BrokerInfo,
    ClusterInfo,
    ConsumerGroupDetail,
    ConsumerGroupMember,
    ConsumerGroupSummary,
    KafkaConnectionSummary,
    OffsetResetResult,
    PartitionInfo,
    PartitionLag,
    PartitionOffsetResetDetail,
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


def _parse_group_item(raw_group: Any) -> tuple[int, str, str, str, str, list[Any]]:
    """将底层多样化消费组描述规整为统一属性元组.

    @param raw_group 底层消费组描述元组、字典或对象
    @return (error_code, group_id, state, protocol_type, protocol, raw_members)
    """
    if isinstance(raw_group, (tuple, list)):
        err = int(raw_group[0]) if len(raw_group) > 0 else 0
        gid = str(raw_group[1]) if len(raw_group) > 1 else ""
        st = str(raw_group[2]) if len(raw_group) > 2 else "Unknown"
        proto_type = str(raw_group[3]) if len(raw_group) > 3 else ""
        proto = str(raw_group[4]) if len(raw_group) > 4 else ""
        members = list(raw_group[5]) if len(raw_group) > 5 else []
        return err, gid, st, proto_type, proto, members
    if isinstance(raw_group, dict):
        err = int(raw_group.get("error_code", 0))
        gid = str(raw_group.get("group") or raw_group.get("group_id") or "")
        st = str(raw_group.get("state", "Unknown"))
        proto_type = str(raw_group.get("protocol_type", ""))
        proto = str(raw_group.get("protocol", ""))
        members = list(raw_group.get("members", []))
        return err, gid, st, proto_type, proto, members
    err = int(getattr(raw_group, "error_code", 0))
    gid = str(getattr(raw_group, "group", getattr(raw_group, "group_id", "")))
    st = str(getattr(raw_group, "state", "Unknown"))
    proto_type = str(getattr(raw_group, "protocol_type", ""))
    proto = str(getattr(raw_group, "protocol", ""))
    members = list(getattr(raw_group, "members", []))
    return err, gid, st, proto_type, proto, members


def _parse_member_item(raw_member: Any) -> tuple[str, str, str, Any]:
    """将底层成员载荷规整为统一标识属性.

    @param raw_member 底层成员元组、字典或对象
    @return (member_id, client_id, client_host, member_assignment)
    """
    if isinstance(raw_member, (tuple, list)):
        mid = str(raw_member[0]) if len(raw_member) > 0 else ""
        cid = str(raw_member[1]) if len(raw_member) > 1 else ""
        chost = str(raw_member[2]) if len(raw_member) > 2 else ""
        massign = raw_member[4] if len(raw_member) > 4 else b""
        return mid, cid, chost, massign
    if isinstance(raw_member, dict):
        mid = str(raw_member.get("member_id", ""))
        cid = str(raw_member.get("client_id", ""))
        chost = str(raw_member.get("client_host", ""))
        massign = raw_member.get("member_assignment", b"")
        return mid, cid, chost, massign
    mid = str(getattr(raw_member, "member_id", ""))
    cid = str(getattr(raw_member, "client_id", ""))
    chost = str(getattr(raw_member, "client_host", ""))
    massign = getattr(raw_member, "member_assignment", b"")
    return mid, cid, chost, massign


def _safe_decode_bytes(raw_bytes: bytes) -> str:
    """安全解码字节流为 UTF-8 文本，失败时自动降级为 Base64 编码字符串.

    @param raw_bytes 待解码字节流
    @return 解码后的字符串
    """
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return base64.b64encode(raw_bytes).decode("ascii")


def decode_payload(
    raw_bytes: bytes | None,
    max_bytes: int = 65536,
) -> tuple[Any, str, int, bool, int | None]:
    """自适应反序列化原始消息载荷字节流，并提供消息截断熔断保护.

    优先尝试解析 UTF-8 JSON 结构化数据；若非合法 JSON 则保留为纯文本；
    若包含无法解码的二进制字节流，则安全降级为 Base64 字符串.
    当载荷超过 max_bytes 阈值时实施截断，降级为前缀纯文本并附带截断元数据.

    @param raw_bytes 消息原始载荷字节流
    @param max_bytes 单条消息最大解码字节上限 (默认 64KB)
    @return (解码后对象或文本, 识别的编码类型, 当前字节大小, 是否截断, 原始完整字节大小)
    """
    if raw_bytes is None:
        return None, "null", 0, False, None

    original_size = len(raw_bytes)
    truncated = False
    original_size_bytes = None

    if max_bytes > 0 and original_size > max_bytes:
        truncated = True
        original_size_bytes = original_size
        slice_bytes = raw_bytes[:max_bytes]
        try:
            text = slice_bytes.decode("utf-8", errors="replace")
            return text, "text", len(slice_bytes), truncated, original_size_bytes
        except Exception:  # noqa: BLE001
            b64_str = base64.b64encode(slice_bytes).decode("ascii")
            return b64_str, "base64", len(slice_bytes), truncated, original_size_bytes

    try:
        text = raw_bytes.decode("utf-8")
        try:
            parsed_json = json.loads(text)
            return parsed_json, "json", original_size, False, None
        except (json.JSONDecodeError, ValueError):
            return text, "text", original_size, False, None
    except UnicodeDecodeError:
        b64_str = base64.b64encode(raw_bytes).decode("ascii")
        return b64_str, "base64", original_size, False, None


def build_ssl_context(config: KafkaConnectionConfig | KafkaConfig) -> ssl.SSLContext | None:
    """自适应构建 SSL/TLS 握手上下文，严格执行 Fail-Fast 凭据存在性校验.

    @param config 包含 SSL 证书与协议配置的对象
    @return 已配置的 SSLContext 实例，非 SSL 协议时返回 None
    @throws FileNotFoundError 当显式指定的证书或私钥文件在磁盘不存在时抛出
    """
    security_protocol = getattr(config, "security_protocol", "PLAINTEXT")
    if security_protocol not in ("SSL", "SASL_SSL"):
        return None

    cafile = getattr(config, "ssl_cafile", None)
    certfile = getattr(config, "ssl_certfile", None)
    keyfile = getattr(config, "ssl_keyfile", None)
    check_hostname = getattr(config, "ssl_check_hostname", True)

    if cafile:
        if not Path(cafile).is_file():
            raise FileNotFoundError(f"指定的 SSL CA 根证书文件不存在或不可读: '{cafile}'")
        context = ssl.create_default_context(cafile=cafile)
    else:
        context = ssl.create_default_context()

    if certfile or keyfile:
        if not certfile or not Path(certfile).is_file():
            raise FileNotFoundError(f"指定的 SSL 客户端证书文件不存在或不可读: '{certfile}'")
        if not keyfile or not Path(keyfile).is_file():
            raise FileNotFoundError(f"指定的 SSL 客户端私钥文件不存在或不可读: '{keyfile}'")
        context.load_cert_chain(certfile=certfile, keyfile=keyfile)

    context.check_hostname = check_hostname
    if not check_hostname:
        context.verify_mode = ssl.CERT_NONE

    return context



def _parse_datetime_to_ms(datetime_val: str | float) -> int:
    """将多样化时间输入格式安全解析为毫秒级 UNIX 时间戳.

    @param datetime_val ISO-8601 字符串、秒级或毫秒级数字
    @return 毫秒级时间戳整数
    @throws ValueError 无法解析时间格式时抛出
    """
    if isinstance(datetime_val, (int, float)):
        ts = int(datetime_val)
        return ts * 1000 if ts < 10_000_000_000 else ts

    s = str(datetime_val).strip()
    if s.isdigit():
        ts = int(s)
        return ts * 1000 if ts < 10_000_000_000 else ts

    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.astimezone()
        return int(dt.timestamp() * 1000)
    except Exception as exc:
        raise ValueError(
            f"无法将时间输入 '{s}' 解析为有效时间戳，支持格式如 '2026-10-04T12:00:00+08:00' 或毫秒时间戳: {exc}"
        ) from exc


class KafkaManager:
    """Kafka 异步客户端与生命周期管理器.

    负责底层 AIOKafkaAdminClient 连接维护、元数据查询与资源回收.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self, config: KafkaConnectionConfig | KafkaConfig) -> None:
        """初始化管理器实例.

        @param config 单集群连接配置或全局运行时配置
        """
        self._config = config
        self._admin_client: AIOKafkaAdminClient | None = None
        self._producer: AIOKafkaProducer | None = None
        self._lock = asyncio.Lock()
        self._ssl_context = build_ssl_context(config)

    @property
    def config(self) -> KafkaConnectionConfig | KafkaConfig:
        """获取当前运行时配置."""
        return self._config

    def _get_security_kwargs(self) -> dict[str, Any]:
        """提取底层 aiokafka 客户端公用的安全鉴权参数字典.

        @return 传给 AIOKafka 构造函数的关键字参数字典
        """
        kwargs: dict[str, Any] = {
            "security_protocol": getattr(self._config, "security_protocol", "PLAINTEXT"),
        }
        sasl_mechanism = getattr(self._config, "sasl_mechanism", None)
        if sasl_mechanism:
            kwargs["sasl_mechanism"] = sasl_mechanism
        sasl_user = getattr(self._config, "sasl_username", None)
        if sasl_user:
            kwargs["sasl_plain_username"] = sasl_user
        sasl_pass = getattr(self._config, "sasl_password", None)
        if sasl_pass:
            kwargs["sasl_plain_password"] = sasl_pass
        if self._ssl_context is not None:
            kwargs["ssl_context"] = self._ssl_context
        return kwargs

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
                    **self._get_security_kwargs(),
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
                    **self._get_security_kwargs(),
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
            [
                (
                    k,
                    v if isinstance(v, bytes) else str(v).encode("utf-8")
                )
                for k, v in headers.items()
            ]
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
        max_bytes_per_message: int = 65536,
    ) -> SampledMessage:
        """构建自适应反序列化后的采样消息实体.

        @param partition 分区编号
        @param offset 消息位移
        @param timestamp 时间戳
        @param key 消息键
        @param raw_value 原始消息载荷字节
        @param headers 消息标头
        @param max_bytes_per_message 单条消息最大解析字节数
        @return 结构化采样消息实体
        """
        # 1. 解码消息键
        key_str: str | None = None
        if key is not None:
            key_str = key if isinstance(key, str) else _safe_decode_bytes(key)

        # 2. 自适应解码载荷与截断熔断
        decoded_val, encoding, size, truncated, original_size_bytes = decode_payload(
            raw_value, max_bytes=max_bytes_per_message
        )

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
            truncated=truncated,
            original_size_bytes=original_size_bytes,
        )


    def create_consumer(
        self,
        group_id: str | None = None,
        enable_auto_commit: bool = False,
    ) -> AIOKafkaConsumer:
        """创建消费者实例 (默认关闭自动提交).

        @param group_id 可选消费组 ID
        @param enable_auto_commit 是否开启自动提交位移，默认 False
        @return 配置好的 AIOKafkaConsumer 实例
        """
        return AIOKafkaConsumer(
            bootstrap_servers=self._config.bootstrap_servers,
            enable_auto_commit=enable_auto_commit,
            group_id=group_id,
            **self._get_security_kwargs(),
        )

    async def sample_messages(
        self,
        topic: str,
        partition: int | None = None,
        strategy: str = "latest",
        offset: int | None = None,
        limit: int = 10,
        max_bytes_per_message: int = 65536,
    ) -> list[SampledMessage]:
        """以零提交位移瞬态方式采样拉取主题消息.

        @param topic 目标主题名称
        @param partition 可选指定分区号
        @param strategy 采样策略 (latest / earliest / offset)
        @param offset 策略为 offset 时的起始数值
        @param limit 采样条数限制 (上限 100)
        @param max_bytes_per_message 单条消息最大解析字节数，超额将实施截断
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
                            max_bytes_per_message=max_bytes_per_message,
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
                    err, g_name, st, _, _, _ = _parse_group_item(g)
                    if err == 0 and g_name:
                        state_map[g_name] = st
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
                _, g_id, _, _, _, _ = _parse_group_item(g)
                if g_id == group_id:
                    target_group = g
                    break
            if target_group is not None:
                break

        if target_group is None:
            raise ValueError(f"消费组 '{group_id}' 不存在或查询失败")

        err_code, _, state, protocol_type, protocol, raw_members = _parse_group_item(target_group)
        if err_code != 0:
            raise ValueError(f"消费组 '{group_id}' 查询返回错误码: {err_code}")

        # 2. 解析活跃成员与分区分配关系
        member_entities: list[ConsumerGroupMember] = []
        member_by_tp: dict[TopicPartition, str] = {}
        assigned_tps_from_members: set[TopicPartition] = set()

        for m in raw_members:
            mid, cid, chost, massign = _parse_member_item(m)
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

        # 补充涉足主题的所有物理分区（单次批量 RPC 消除 N+1 网络往返）
        involved_topics = {tp.topic for tp in all_tps if tp.topic in existing_topic_names}
        if involved_topics:
            try:
                raw_topics = await admin.describe_topics(list(involved_topics))
                for raw_top in raw_topics:
                    t_detail = _parse_topic_detail(raw_top)
                    for part in t_detail.partitions:
                        all_tps.add(TopicPartition(t_detail.name, part.partition_id))
            except Exception as exc:  # noqa: BLE001
                logger.debug("批量获取涉足主题物理分区扩展信息失败: %s", exc)

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

    async def reset_consumer_group_offsets(
        self,
        group_id: str,
        topic: str,
        strategy: str,
        offset: int | None = None,
        datetime_val: str | float | None = None,
        partitions: list[int] | None = None,
        dry_run: bool = True,
        force: bool = False,
    ) -> OffsetResetResult:
        """重置指定消费组在目标主题上的消费位移 (支持 Dry-Run 预检与活跃冲突防御).

        @param group_id 目标消费组 ID
        @param topic 目标主题名
        @param strategy 重置策略 (earliest, latest, to_offset, to_datetime)
        @param offset 当 strategy=to_offset 时的目标位移数值
        @param datetime_val 当 strategy=to_datetime 时的 ISO 时间串或毫秒时间戳
        @param partitions 可选指定重置的分区列表，为 None 时针对该主题所有物理分区
        @param dry_run 是否仅预检评估，默认 True
        @param force 是否强制重置活跃状态下的消费组，默认 False
        @return 包含每个分区调整前后位移与差值的计算结果实体
        @throws ValueError 参数非法或活跃消费组冲突拦截时抛出
        """
        # 1. 前置状态探测：检查消费组活跃性
        admin = await self.get_admin_client()
        warning: str | None = None
        try:
            group_desc_list = await admin.describe_consumer_groups([group_id])
            if group_desc_list:
                grp_desc = group_desc_list[0]
                state = getattr(grp_desc, "state", "Unknown")
                members = getattr(grp_desc, "members", [])
                if state in ("Stable", "PreparingRebalance", "CompletingRebalance") and len(members) > 0:
                    warning = (
                        f"消费组 '{group_id}' 当前处于活跃运行状态 ({state}, 活跃成员数: {len(members)})，"
                        "直接重置位移可能被下游消费客户端的心跳与提交覆盖！"
                    )
                    if not dry_run and not force:
                        raise ValueError(f"{warning} 建议先停机下游消费者，或在确认影响后传入 force=True 强制执行。")
        except ValueError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.debug("获取消费组 %s 状态用于活跃检查失败 (可能为空组): %s", group_id, exc)

        # 2. 获取目标主题的所有物理分区
        consumer = self.create_consumer()
        await consumer.start()
        try:
            all_parts = await consumer.partitions_for_topic(topic)
            if not all_parts:
                raise ValueError(f"主题 '{topic}' 不存在或当前没有任何可用物理分区")

            target_part_ids = sorted(
                list(all_parts) if partitions is None else [p for p in partitions if p in all_parts]
            )
            if not target_part_ids:
                raise ValueError(f"指定的分区列表 {partitions} 与主题 '{topic}' 的现有物理分区 {sorted(all_parts)} 无交集")

            target_tps = [TopicPartition(topic, p) for p in target_part_ids]

            # 3. 获取各分区当前已提交位移
            committed_map = await admin.list_consumer_group_offsets(group_id)
            current_offsets: dict[int, int] = {}
            for tp in target_tps:
                comm = committed_map.get(tp)
                if comm is not None:
                    curr_off = getattr(comm, "offset", None)
                    if curr_off is None and isinstance(comm, (tuple, list)) and len(comm) > 0:
                        curr_off = comm[0]
                    current_offsets[tp.partition] = curr_off if curr_off is not None else 0
                else:
                    current_offsets[tp.partition] = 0

            # 4. 根据策略计算 target_offsets
            target_offsets: dict[int, int] = {}
            strategy_lower = strategy.strip().lower()

            if strategy_lower == "earliest":
                beg_offsets = await consumer.beginning_offsets(target_tps)
                for tp in target_tps:
                    target_offsets[tp.partition] = beg_offsets.get(tp, 0)

            elif strategy_lower == "latest":
                end_offsets = await consumer.end_offsets(target_tps)
                for tp in target_tps:
                    target_offsets[tp.partition] = end_offsets.get(tp, 0)

            elif strategy_lower == "to_offset":
                if offset is None:
                    raise ValueError("strategy='to_offset' 必须显式传入 offset 整数参数")
                target_offset_val = max(0, offset)
                # 位移上界约束 (Offset Upper Bound Guard)：校验目标位移不得超越分区当前 LEO
                end_offsets = await consumer.end_offsets(target_tps)
                over_leo_errors: list[str] = []
                for tp in target_tps:
                    leo = end_offsets.get(tp, 0)
                    if target_offset_val > leo:
                        over_leo_errors.append(f"分区 {tp.partition} (当前 LEO: {leo})")
                if over_leo_errors:
                    raise ValueError(
                        f"目标重置位移 {target_offset_val} 超出分区日志末端位移 (LEO) 上界，已触发原子性拦截防御: "
                        + ", ".join(over_leo_errors)
                        + "。严禁设置未来位移以防消息永久丢失。"
                    )
                for tp in target_tps:
                    target_offsets[tp.partition] = target_offset_val

            elif strategy_lower == "to_datetime":
                if datetime_val is None:
                    raise ValueError("strategy='to_datetime' 必须显式传入 datetime_val 参数")
                target_ts_ms = _parse_datetime_to_ms(datetime_val)
                times_query = {tp: target_ts_ms for tp in target_tps}
                found_offsets = await consumer.offsets_for_times(times_query)
                end_offsets = await consumer.end_offsets(target_tps)
                for tp in target_tps:
                    found = found_offsets.get(tp)
                    if found is not None and getattr(found, "offset", None) is not None:
                        target_offsets[tp.partition] = found.offset
                    else:
                        target_offsets[tp.partition] = end_offsets.get(tp, 0)
            else:
                raise ValueError(
                    f"不支持的位移重置策略: '{strategy}'，可选值: earliest, latest, to_offset, to_datetime"
                )

            # 5. 构建每个分区的明细列表与总变动差值
            details: list[PartitionOffsetResetDetail] = []
            total_delta = 0
            for p in target_part_ids:
                curr = current_offsets[p]
                targ = target_offsets[p]
                delta = targ - curr
                total_delta += delta
                details.append(
                    PartitionOffsetResetDetail(
                        partition=p,
                        current_offset=curr,
                        target_offset=targ,
                        offset_delta=delta,
                    )
                )

        finally:
            await consumer.stop()

        # 6. 若非 dry_run，真正落盘提交位移
        applied = False
        if not dry_run:
            commit_consumer = self.create_consumer(group_id=group_id, enable_auto_commit=False)
            await commit_consumer.start()
            try:
                commit_consumer.assign(target_tps)
                commit_dict = {
                    tp: OffsetAndMetadata(target_offsets[tp.partition], "mcp-server-kafka reset")
                    for tp in target_tps
                }
                await commit_consumer.commit(commit_dict)
                applied = True
            finally:
                await commit_consumer.stop()

        return OffsetResetResult(
            group_id=group_id,
            topic=topic,
            strategy=strategy_lower,
            dry_run=dry_run,
            applied=applied,
            warning=warning,
            partitions=details,
            total_partitions=len(details),
            total_delta=total_delta,
        )


class KafkaManagerRegistry:
    """多集群 Kafka 管理器注册中心 (Connection Registry).

    负责统一管理各命名连接的 KafkaManager 实例生命周期，提供无状态动态路由与只读安全校验.

    @author Ateng
    @since 2026-10-04
    """

    def __init__(self, global_config: KafkaConfig) -> None:
        """基于全局配置初始化连接注册中心.

        @param global_config 全局配置对象
        """
        self._global_config = global_config
        self._managers: dict[str, KafkaManager] = {}

        # 遍历配置中的各命名连接，初始化独立的 KafkaManager
        for name, conn_cfg in global_config.connections.items():
            self._managers[name] = KafkaManager(conn_cfg)

    @property
    def global_config(self) -> KafkaConfig:
        """获取系统全局运行时配置."""
        return self._global_config

    @property
    def default_connection_name(self) -> str:
        """获取当前配置的默认集群连接名称."""
        return self._global_config.default_connection

    def get_manager(self, connection_name: str | None = None) -> KafkaManager:
        """根据连接别名获取对应的 KafkaManager 实例.

        @param connection_name 集群连接别名，为空或 None 时自动回退至 default_connection
        @return 对应的 KafkaManager 实例
        @throws ValueError 当指定别名未配置时抛出
        """
        target = (
            connection_name.strip()
            if connection_name and connection_name.strip()
            else self.default_connection_name
        )
        if target not in self._managers:
            valid_names = list(self._managers.keys())
            raise ValueError(
                f"未找到名为 '{target}' 的 Kafka 连接配置，当前系统已配置的可用连接为: {valid_names}"
            )
        return self._managers[target]

    def is_connection_read_only(self, connection_name: str | None = None) -> bool:
        """判断指定连接是否处于只读保护状态（全局只读或连接级只读）.

        @param connection_name 集群连接别名
        @return 若处于只读保护返回 True，否则返回 False
        """
        if self._global_config.read_only:
            return True
        target = (
            connection_name.strip()
            if connection_name and connection_name.strip()
            else self.default_connection_name
        )
        if target in self._global_config.connections:
            return self._global_config.connections[target].read_only
        return False

    def list_connections(self) -> list[KafkaConnectionSummary]:
        """获取所有已配置集群连接的元数据摘要列表.

        @return 连接元数据实体列表（保证非空集合）
        """
        result: list[KafkaConnectionSummary] = []
        for name, conn_cfg in self._global_config.connections.items():
            is_def = (name == self.default_connection_name)
            is_ro = bool(self._global_config.read_only or conn_cfg.read_only)
            result.append(
                KafkaConnectionSummary(
                    name=name,
                    bootstrap_servers=conn_cfg.bootstrap_servers,
                    read_only=is_ro,
                    is_default=is_def,
                    security_protocol=conn_cfg.security_protocol,
                    sasl_mechanism=conn_cfg.sasl_mechanism,
                )
            )
        return result

    async def close_all(self) -> None:
        """安全关闭并释放注册表中所有集群管理器的底层网络连接与生产通道."""
        for mgr in self._managers.values():
            await mgr.close()



