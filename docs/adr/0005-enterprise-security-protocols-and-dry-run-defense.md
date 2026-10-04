# 0005: 企业级安全认证协议矩阵与预检试运行防线架构设计

针对生产环境公有云托管 Kafka 集群与企业自建集群的鉴权加密诉求，我们决定在命名连接配置模型中原生引入企业级安全认证协议矩阵（SASL/SSL/SCRAM/mTLS），并在高危运维工具中建立“三级防线 + 预检试运行 (Dry-Run)”的主动安全防御体系。

## 决策背景

在企业实际落地中，绝大多数公有云（AWS MSK、阿里云/腾讯云 Kafka、Confluent Cloud）或内网高安全隔离集群均强制开启了网络加密与身份鉴权：
1. **认证协议碎片化**：常用的包括 `SASL_PLAINTEXT`、`SASL_SSL`，以及基于 `PLAIN`、`SCRAM-SHA-256`、`SCRAM-SHA-512` 等不同的 SASL 机制，或通过私有 CA 签发证书进行 mTLS 双向认证；
2. **机密凭证安全泄露风险**：若直接在 YAML 配置文件中硬编码密码或密钥，极易造成凭证泄露；需要原生支持环境变量引用插值（如 `${KAFKA_PASSWORD}`）；
3. **高危运维带来的灾难性隐患**：随着未来工具集拓展至消费组位移重置等高级管理能力，AI 智能体直接触发真实重置可能瞬间导致线上消费者重复消费或丢失数据，仅依赖 `confirm=True` 依然存在智能体盲目确认的隐患，必须引入可观测的 Dry-Run 预检试运行。

## 决策内容

1. **多协议声明式认证扩展 (Declarative Security Schema)**：
   - 在 `KafkaConnectionConfig` 模型中追加安全属性：
     - `security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"`
     - `sasl_mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"] | None = None`
     - `sasl_username: str | None = None`
     - `sasl_password: str | None = None`
     - `ssl_cafile: str | None = None`（CA 证书路径）
     - `ssl_certfile: str | None = None`（客户端证书）
     - `ssl_keyfile: str | None = None`（客户端私钥）
     - `ssl_check_hostname: bool = True`（支持在私有自签证书调试时灵活开关）
   - 单集群兼容：在单连接启动时，透传支持环境变量 `MCP_KAFKA_SECURITY_PROTOCOL`、`MCP_KAFKA_SASL_USERNAME`、`MCP_KAFKA_SASL_PASSWORD` 等。

2. **环境变量插值引擎 (Environment Variable Interpolation)**：
   - 配置文件解析器加载 YAML 内容时，支持 `${VAR_NAME}` 语法动态提取宿主系统环境变量；
   - 实行严格的 Fail-Fast 校验：若配置了必填插值变量但在环境中不存在，直接在服务启动期报错阻断，杜绝以空密码建立无效连接。

3. **三级纵深防御与预检试运行 (Dry-Run Defense-in-Depth)**：
   - **一级防线（全局只读）**：`--read-only` 开启时，一切涉及位移重置、主题创建/删除等变更类操作直接隐藏阻断；
   - **二级防线（连接级只读）**：生产连接单独配置 `read_only: true` 时，细粒度拦截对该连接的任何写操作；
   - **三级防线（预检试运行与二次确认）**：
     - 高危工具（如后续拓展的位移重置）默认参数设为 `dry_run: bool = True, confirm: bool = False`；
     - 当 `dry_run=True` 时，底层仅执行位移差值、受影响分区及预期重置位置的计算并返回详细评估报告，不向 Kafka 提交任何真实变更；
     - 仅当调用方显式传入 `dry_run=False, confirm=True` 双重授权时，系统才真正执行落盘变更。
