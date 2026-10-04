"""Kafka MCP 服务配置与多连接参数解析.

@author Ateng
@since 2026-10-04
"""

import argparse
import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z0-9_]+)\}")


def interpolate_env_vars(val: Any) -> Any:
    """递归对字符串、字典或列表中的 ${ENV_VAR} 执行环境变量插值.

    @param val 待处理的数据（可为 str, dict, list 或其他基本类型）
    @return 替换环境变量后的数据
    @throws ValueError 若指定的环境变量在当前系统中不存在则抛出异常 (Fail-Fast)
    """
    if isinstance(val, str):
        def _replace_match(match: re.Match[str]) -> str:
            var_name = match.group(1)
            env_val = os.getenv(var_name)
            if env_val is None:
                raise ValueError(f"配置文件中引用的环境变量 '${{{var_name}}}' 在当前系统中未定义")
            return env_val

        return _ENV_VAR_PATTERN.sub(_replace_match, val)
    if isinstance(val, dict):
        return {k: interpolate_env_vars(v) for k, v in val.items()}
    if isinstance(val, list):
        return [interpolate_env_vars(item) for item in val]
    return val


class KafkaConnectionConfig(BaseModel):
    """单套 Kafka 集群连接配置模型.

    @author Ateng
    @since 2026-10-04
    """

    name: str = Field(description="集群连接逻辑别名")
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka 集群 Broker 连接地址列表 (多个以逗号分隔)",
    )
    read_only: bool = Field(
        default=False,
        description="本连接实例只读防线开关，开启后拒绝一切写操作",
    )
    security_protocol: str = Field(
        default="PLAINTEXT",
        description="通信安全协议 (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)",
    )
    sasl_mechanism: str | None = Field(
        default=None,
        description="SASL 鉴权机制 (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI 等)",
    )
    sasl_username: str | None = Field(
        default=None,
        description="SASL 认证用户名",
    )
    sasl_password: str | None = Field(
        default=None,
        description="SASL 认证密码 (支持 ${ENV} 环境变量插值)",
    )
    ssl_cafile: str | None = Field(
        default=None,
        description="自定义根证书/CA 证书文件路径",
    )
    ssl_certfile: str | None = Field(
        default=None,
        description="客户端 SSL 证书路径 (用于双向 mTLS 认证)",
    )
    ssl_keyfile: str | None = Field(
        default=None,
        description="客户端 SSL 私钥路径 (用于双向 mTLS 认证)",
    )
    ssl_check_hostname: bool = Field(
        default=True,
        description="是否对 SSL 证书进行主机名/域名强校验",
    )


class KafkaConfig(BaseModel):
    """Kafka 服务运行时整体配置模型.

    支持单集群命令行直接启动与多集群 YAML 配置文件声明双轨运行模式.

    @author Ateng
    @since 2026-10-04
    """

    config_file: str | None = Field(
        default=None,
        description="多连接 YAML 配置文件路径",
    )
    default_connection: str = Field(
        default="default",
        description="全局默认连接别名",
    )
    read_only: bool = Field(
        default=False,
        description="全局只读防线开关，开启后对所有连接统一生效",
    )
    connections: dict[str, KafkaConnectionConfig] = Field(
        default_factory=dict,
        description="命名 Kafka 集群连接字典映射",
    )
    transport: str = Field(
        default="stdio",
        description="传输协议网关类型 (stdio / sse)",
    )
    host: str = Field(
        default="0.0.0.0",
        description="HTTP SSE 网关监听主机地址",
    )
    port: int = Field(
        default=8000,
        description="HTTP SSE 网关监听端口号",
    )
    log_level: str = Field(
        default="INFO",
        description="服务运行日志级别 (DEBUG / INFO / WARNING / ERROR)",
    )

    @property
    def bootstrap_servers(self) -> str:
        """获取默认连接的 Broker 引导地址（向后兼容传统单连接访问）."""
        if self.default_connection in self.connections:
            return self.connections[self.default_connection].bootstrap_servers
        if self.connections:
            first_conn = next(iter(self.connections.values()))
            return first_conn.bootstrap_servers
        return "localhost:9092"

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """从系统环境变量构建基础配置实例.

        @return 运行时配置对象
        """
        config_file = os.getenv("MCP_KAFKA_CONFIG")
        servers = os.getenv("MCP_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        read_only_raw = os.getenv("MCP_KAFKA_READ_ONLY", "false").strip().lower()
        read_only = read_only_raw in ("true", "1", "yes", "on")

        security_protocol = os.getenv("MCP_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip().upper()
        sasl_mechanism_raw = os.getenv("MCP_KAFKA_SASL_MECHANISM")
        sasl_mechanism = sasl_mechanism_raw.strip().upper() if sasl_mechanism_raw else None
        sasl_username = os.getenv("MCP_KAFKA_SASL_USERNAME")
        sasl_password = os.getenv("MCP_KAFKA_SASL_PASSWORD")
        ssl_cafile = os.getenv("MCP_KAFKA_SSL_CAFILE")
        ssl_certfile = os.getenv("MCP_KAFKA_SSL_CERTFILE")
        ssl_keyfile = os.getenv("MCP_KAFKA_SSL_KEYFILE")
        ssl_check_raw = os.getenv("MCP_KAFKA_SSL_CHECK_HOSTNAME", "true").strip().lower()
        ssl_check_hostname = ssl_check_raw not in ("false", "0", "no", "off")

        transport = os.getenv("MCP_KAFKA_TRANSPORT", "stdio").strip().lower()
        if transport not in ("stdio", "sse"):
            transport = "stdio"

        host = os.getenv("MCP_KAFKA_SERVER_HOST", "0.0.0.0").strip()
        port_raw = os.getenv("MCP_KAFKA_SERVER_PORT", "8000").strip()
        try:
            port = int(port_raw)
        except ValueError:
            port = 8000

        log_level = os.getenv("MCP_KAFKA_LOG_LEVEL", "INFO").strip().upper()

        # 默认生成单连接结构
        default_conn = KafkaConnectionConfig(
            name="default",
            bootstrap_servers=servers,
            read_only=read_only,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_check_hostname=ssl_check_hostname,
        )

        return cls(
            config_file=config_file,
            default_connection="default",
            read_only=read_only,
            connections={"default": default_conn},
            transport=transport,
            host=host,
            port=port,
            log_level=log_level,
        )


def _load_yaml_file(file_path: str) -> dict[str, Any]:
    """安全读取并解析 YAML 配置文件，并递归执行环境变量插值.

    @param file_path 配置文件相对或绝对路径
    @return 解析并插值后的字典载荷
    @throws FileNotFoundError 当配置文件不存在时抛出
    @throws ValueError 当环境变量缺失时抛出
    @throws TypeError 当内容格式非合法字典映射时抛出
    """
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"指定的 Kafka MCP 配置文件不存在: {file_path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise TypeError(f"配置文件 {file_path} 格式非法，必须为顶级 YAML 字典映射")
    return interpolate_env_vars(data)


def parse_cli_args(args: list[str] | None = None) -> KafkaConfig:
    """解析命令行参数、环境变量并与 YAML 配置文件深度合并.

    优先级规则：命令行参数 > YAML 配置文件 > 环境变量 > 默认值.

    @param args 传入的命令行参数列表，为 None 时由 argparse 从 sys.argv 解析
    @return 最终生效的 Kafka 运行时配置
    """
    env_config = KafkaConfig.from_env()
    env_default_conn = env_config.connections.get("default")

    parser = argparse.ArgumentParser(description="atengk-mcp-server-kafka: Apache Kafka MCP 服务端")
    parser.add_argument(
        "-c",
        "--config",
        dest="config_file",
        default=env_config.config_file,
        help="多集群连接 YAML 配置文件路径 (默认: 环境变量 MCP_KAFKA_CONFIG)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help="Kafka 集群 Broker 连接地址列表 (未指定 --config 时生效，默认: localhost:9092)",
    )
    parser.add_argument(
        "--read-only",
        action="store_true",
        default=False,
        help="启用全局只读模式，屏蔽一切写操作 (优先级最高)",
    )
    parser.add_argument(
        "--security-protocol",
        choices=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"],
        default=env_default_conn.security_protocol if env_default_conn else "PLAINTEXT",
        help="安全通信协议 (默认: PLAINTEXT)",
    )
    parser.add_argument(
        "--sasl-mechanism",
        choices=["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"],
        default=env_default_conn.sasl_mechanism if env_default_conn else None,
        help="SASL 认证算法机制",
    )
    parser.add_argument(
        "--sasl-username",
        default=env_default_conn.sasl_username if env_default_conn else None,
        help="SASL 认证用户名",
    )
    parser.add_argument(
        "--sasl-password",
        default=env_default_conn.sasl_password if env_default_conn else None,
        help="SASL 认证密码",
    )
    parser.add_argument(
        "--ssl-cafile",
        default=env_default_conn.ssl_cafile if env_default_conn else None,
        help="SSL 自定义 CA 证书文件路径",
    )
    parser.add_argument(
        "--ssl-certfile",
        default=env_default_conn.ssl_certfile if env_default_conn else None,
        help="客户端 SSL 证书文件路径 (mTLS)",
    )
    parser.add_argument(
        "--ssl-keyfile",
        default=env_default_conn.ssl_keyfile if env_default_conn else None,
        help="客户端 SSL 私钥文件路径 (mTLS)",
    )
    parser.add_argument(
        "--no-ssl-check-hostname",
        action="store_true",
        default=False,
        help="禁用 SSL 证书主机名强校验 (用于自建内网集群测试)",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default=env_config.transport,
        help="传输协议网关类型 (默认: 环境变量或 stdio)",
    )
    parser.add_argument(
        "--host",
        default=env_config.host,
        help="HTTP SSE 网关监听主机地址 (默认: 环境变量或 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=env_config.port,
        help="HTTP SSE 网关监听端口号 (默认: 环境变量或 8000)",
    )
    parser.add_argument(
        "--log-level",
        default=env_config.log_level,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="服务运行日志级别 (默认: 环境变量或 INFO)",
    )

    parsed = parser.parse_args(args)

    # 1. 确定最终全局只读开关状态（环境变量与 CLI 任意开启即开启）
    effective_read_only = bool(parsed.read_only or env_config.read_only)

    # 2. 判断是否指定了 YAML 配置文件
    config_file = parsed.config_file
    if config_file:
        yaml_data = _load_yaml_file(config_file)
        default_conn_name = str(yaml_data.get("default_connection", "default"))
        yaml_read_only = bool(yaml_data.get("read_only", False))
        if yaml_read_only:
            effective_read_only = True

        raw_connections = yaml_data.get("connections", {})
        connections: dict[str, KafkaConnectionConfig] = {}

        if isinstance(raw_connections, dict):
            for name, item in raw_connections.items():
                if isinstance(item, dict):
                    c_name = str(item.get("name", name))
                    c_servers = str(item.get("bootstrap_servers", "localhost:9092"))
                    c_read_only = bool(item.get("read_only", False)) or effective_read_only
                    c_sec_proto = str(item.get("security_protocol", "PLAINTEXT")).upper()
                    c_sasl_mech = str(item["sasl_mechanism"]).upper() if item.get("sasl_mechanism") else None
                    c_sasl_user = str(item["sasl_username"]) if item.get("sasl_username") is not None else None
                    c_sasl_pass = str(item["sasl_password"]) if item.get("sasl_password") is not None else None
                    c_ssl_ca = str(item["ssl_cafile"]) if item.get("ssl_cafile") is not None else None
                    c_ssl_cert = str(item["ssl_certfile"]) if item.get("ssl_certfile") is not None else None
                    c_ssl_key = str(item["ssl_keyfile"]) if item.get("ssl_keyfile") is not None else None
                    c_ssl_check = bool(item.get("ssl_check_hostname", True))

                    connections[c_name] = KafkaConnectionConfig(
                        name=c_name,
                        bootstrap_servers=c_servers,
                        read_only=c_read_only,
                        security_protocol=c_sec_proto,
                        sasl_mechanism=c_sasl_mech,
                        sasl_username=c_sasl_user,
                        sasl_password=c_sasl_pass,
                        ssl_cafile=c_ssl_ca,
                        ssl_certfile=c_ssl_cert,
                        ssl_keyfile=c_ssl_key,
                        ssl_check_hostname=c_ssl_check,
                    )

        # 兜底：若 YAML 中 connections 为空，则回退为默认单连接
        if not connections:
            connections["default"] = KafkaConnectionConfig(
                name="default",
                bootstrap_servers="localhost:9092",
                read_only=effective_read_only,
            )
            default_conn_name = "default"
        elif default_conn_name not in connections:
            # 若指定的 default_connection 未在字典中，默认选取第一个可用连接
            default_conn_name = next(iter(connections.keys()))

        return KafkaConfig(
            config_file=config_file,
            default_connection=default_conn_name,
            read_only=effective_read_only,
            connections=connections,
            transport=parsed.transport,
            host=parsed.host,
            port=parsed.port,
            log_level=parsed.log_level,
        )

    # 3. 未指定 YAML 配置文件，进入传统单连接命令行模式
    servers = parsed.bootstrap_servers or env_config.bootstrap_servers
    ssl_check_hostname = not parsed.no_ssl_check_hostname
    default_conn = KafkaConnectionConfig(
        name="default",
        bootstrap_servers=servers,
        read_only=effective_read_only,
        security_protocol=parsed.security_protocol,
        sasl_mechanism=parsed.sasl_mechanism,
        sasl_username=parsed.sasl_username,
        sasl_password=parsed.sasl_password,
        ssl_cafile=parsed.ssl_cafile,
        ssl_certfile=parsed.ssl_certfile,
        ssl_keyfile=parsed.ssl_keyfile,
        ssl_check_hostname=ssl_check_hostname,
    )

    return KafkaConfig(
        config_file=None,
        default_connection="default",
        read_only=effective_read_only,
        connections={"default": default_conn},
        transport=parsed.transport,
        host=parsed.host,
        port=parsed.port,
        log_level=parsed.log_level,
    )
