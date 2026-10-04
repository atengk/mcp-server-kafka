# mcp-server-kafka

<p align="center">
  <strong>基于 Python 与 FastMCP 构建的生产级 Apache Kafka 模型上下文协议服务端</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/atengk-mcp-server-kafka/">
    <img src="https://img.shields.io/pypi/v/atengk-mcp-server-kafka?style=flat-square&color=blue" alt="PyPI Version" />
  </a>
  <a href="https://pypi.org/project/atengk-mcp-server-kafka/">
    <img src="https://img.shields.io/pypi/pyversions/atengk-mcp-server-kafka?style=flat-square" alt="Python Versions" />
  </a>
  <a href="https://github.com/atengk/mcp-server-kafka/actions/workflows/ci.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/atengk/mcp-server-kafka/ci.yml?branch=main&label=CI&style=flat-square" alt="CI Status" />
  </a>
  <a href="https://github.com/atengk/mcp-server-kafka/releases">
    <img src="https://img.shields.io/github/v/release/atengk/mcp-server-kafka?style=flat-square" alt="Release" />
  </a>
  <a href="./LICENSE">
    <img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg?style=flat-square" alt="License" />
  </a>
  <a href="./CONTRIBUTING.md">
    <img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square" alt="PRs Welcome" />
  </a>
</p>

---

## 📖 项目简介

`mcp-server-kafka` 是一个严格遵循 [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) 规范的 Apache Kafka 服务端集成。通过标准化的协议接口，赋予大型语言模型（LLM）与 AI Agent 安全、高效地对 Kafka 分布式消息集群开展元数据只读探查、消息生产、无侵入瞬态消息采样拉取、消费组监控与积压 (Lag) 根因诊断能力。

本项目采用纯异步事件驱动架构，内建**双重安全防线**，杜绝 AI 智能体误操作对生产集群造成破坏。

---

## ✨ 核心特性

- 🎯 **现代 Python 技术栈**：基于 Python 3.10+、FastMCP 框架与 `uv` 极速包管理器构建，遵循 PEP 621 标准；
- ⚡ **纯异步高性能驱动**：全面采用 `aiokafka` 异步通信与 Admin 接口，高并发无阻塞；
- 🔀 **多集群命名连接路由**：支持通过 `--config connections.yaml` 统一声明多套环境实例（如 default、staging、production），实现单进程内无状态动态调度；
- 🔐 **企业级安全认证协议矩阵**：全面支持 `PLAINTEXT`、`SSL` (mTLS 双向证书)、`SASL_PLAINTEXT` 与 `SASL_SSL`（覆盖 `PLAIN`、`SCRAM-SHA-256`、`SCRAM-SHA-512` 等），支持 `${ENV_VAR}` 环境变量动态插值防范明文泄露；
- 🛡️ **三级纵深安全防线与 Dry-Run 预检**：
  - **一级防线（全局只读）**：开启 `--read-only` 模式时，一切变更类工具自动隐藏并阻断执行；
  - **二级防线（连接级隔离）**：支持为生产连接单独配置 `read_only: true` 实施实例级只读写保护；
  - **三级防线（预检试运行 & 二次确认）**：高危位移重置支持 `dry_run=True` 预检评估与 `confirm=True` 二次确认，自动拦截活跃消费组状态冲突；
- 🔍 **零位移侵入瞬态采样**：采样读取消息时不加入消费组、强制不向 `__consumer_offsets` 提交位移，绝不破坏生产环境业务消费进度；
- 🧩 **自适应内容解析**：消息载荷优先解析为结构化 JSON 或 UTF-8 文本，二进制载荷安全降级为 Base64；
- 🌐 **Stdio / HTTP SSE 双模传输**：完美兼顾本地单机桌面客户端（如 Claude Desktop）与云原生容器常驻运行；
- 🚀 **开箱即用容器化**：提供精简多阶段 Dockerfile（无 root 用户 `appuser`）与 docker-compose 编排。

---

## 🛠️ MCP 协议能力清单 (Tools, Resources, Prompts)

### 1. Tools 工具集 (11 项)

| 工具名称 | 功能描述 | 核心入参 | 安全策略 |
| :--- | :--- | :--- | :--- |
| `kafka_list_connections` | 枚举当前服务端已配置的所有 Kafka 集群命名连接清单、安全协议及只读状态 | 无 | 只读安全 |
| `kafka_cluster_info` | 查询 Kafka 集群元数据与 Broker 节点列表 | `connection`（可选集群别名） | 只读安全 |
| `kafka_list_topics` | 列出集群主题清单及分区数 | `pattern`, `include_internal`, `connection` | 只读安全 |
| `kafka_describe_topic` | 查询指定主题的分区分布、Leader 节点、ISR 与自定义配置 | `topic_name`, `connection` | 只读安全 |
| `kafka_create_topic` | 创建新主题 | `topic_name`, `partitions`, `replication_factor`, `connection` | 只读模式或生产连接下自动拦截 |
| `kafka_delete_topic` | 删除指定主题 | `topic_name`, `confirm: bool = False`, `connection` | 强制 `confirm=True` 二次确认；只读或生产连接下拦截 |
| `kafka_produce_message` | 向指定主题发送消息（支持字典/列表 JSON、文本与 Headers） | `topic`, `value`, `key`, `partition`, `headers`, `connection` | 只读模式或生产连接下自动拦截 |
| `kafka_sample_messages` | 零提交位移瞬态采样读取消息，自适应格式解码并支持大报文截断熔断 | `topic`, `partition`, `strategy`, `offset`, `limit`, `max_bytes_per_message`, `connection` | 只读安全 (默认 64KB 截断熔断，单次上限 100 条) |
| `kafka_list_consumer_groups` | 枚举集群中所有消费组 ID、协议类型与运行状态 | `connection` | 只读安全 |
| `kafka_describe_consumer_group` | 查询消费组各分区 Committed Offset、LEO、Lag 及活跃成员分配 | `group_id`, `connection` | 只读安全 (单次批量 RPC 拓扑，含已删除主题防御) |
| `kafka_reset_consumer_group_offsets` | 重置指定消费组位移（支持 earliest, latest, to_offset, to_datetime） | `group_id`, `topic`, `strategy`, `offset`, `datetime_val`, `partitions`, `dry_run`, `confirm`, `force`, `connection` | 默认 `dry_run=True` 预检；只读模式下开放预检并拦截物理提交；活跃组冲突拦截 |

### 2. Resources 资源集 (2 项)

| 资源 URI | 描述 | MIME 类型 |
| :--- | :--- | :--- |
| `kafka://cluster/summary` | 以只读上下文形式输出当前 Kafka 集群与 Broker 运行节点摘要 | `text/plain` |
| `kafka://topics/{topic}` | 动态输出指定 Kafka 主题的分区、Leader 与副本分布明细 | `text/plain` |

### 3. Prompts 提示词模板 (2 项)

| 提示词名称 | 描述 | 关键参数 |
| :--- | :--- | :--- |
| `diagnose_topic_lag` | 引导模型从分区倾斜 (Partition Skew)、成员断连 (Consumer Dead)、心跳超时等维度展开消费积压根因诊断 | `group_id`, `topic` (可选) |
| `inspect_topic_messages` | 引导模型安全采样消息，对载荷格式、Schema 一致性、数据质量与路由键开展探查分析 | `topic`, `limit`, `strategy` |

---

### 4. 🧭 消费组位移治理与回溯实操指南 (`kafka_reset_consumer_group_offsets`)

`kafka_reset_consumer_group_offsets` 是针对线上故障快速止血、历史数据补录或逻辑修复回溯设计的企业级运维工具。为了杜绝大模型误操作引发重复消费风暴或数据丢失，设计了**严密的三级纵深防线**：

```
                    ┌──────────────────────────────────────────────┐
                    │  kafka_reset_consumer_group_offsets 工具调用  │
                    └──────────────────────┬───────────────────────┘
                                           │
                                ┌──────────┴──────────┐
                     [dry_run=True (默认)]      [dry_run=False (执行)]
                                │                         │
                        只读试运行模拟             是否为只读集群/只读连接?
                        计算 Lag / 变更前后              ├── 是 ──> 强制拦截拒绝物理提交
                                │                         └── 否
                        安全返回预检报告 (无写入)            是否显式 confirm=True?
                                                          ├── 否 ──> 抛出拦截警示
                                                          └── 是
                                                                   │
                                                    消费组是否处于 Stable 且存在活跃成员?
                                                           ├── 是 ──> 是否 force=True?
                                                           │            ├── 否 ──> 拦截拒绝 (防止状态冲突)
                                                           │            └── 是 ──> 物理提交位移
                                                           └── 否 ──> 物理提交位移并返回结果
```

#### A. 四大重置策略
- **`earliest`**：回滚至最早可用位移（用于数据全量重新消费）；
- **`latest`**：跳跃至最新分区末端位移（用于故障积压直接跳过、快速追齐实时数据）；
- **`to_offset`**：指定具体的绝对位移数值（需配合 `offset` 参数；受**位移上界约束 (Offset Upper Bound Guard)** 保护，严禁设置超越分区 LEO 的未来位移以防消息永久丢失）；
- **`to_datetime`**：回溯至指定历史时间点（配合 `datetime_val`，支持 ISO-8601 字符串如 `"2026-10-04T12:00:00+08:00"` 或毫秒时间戳如 `1791100000000`）。

#### B. 标准运维实操工作流
1. **阶段 1：预检评估 (Dry-Run)**  
   默认 `dry_run=True`，服务端仅查询当前 Committed Offset、目标 New Offset 与预计 Lag 变动，**绝对不向 Kafka 提交任何位移**（在全局 `--read-only` 模式下亦完全开放此安全预检能力）：
   > *“帮我预检一下将 order-group 消费组在 order-events 主题上的位移重置到 2026-10-04 12:00:00 的效果。”*
2. **阶段 2：人工审查确认后物理提交**  
   审查预检报告确认无误后，显式传入 `dry_run=False, confirm=True` 完成物理提交：
   > *“确认预检结果符合预期，请正式执行位移重置 (confirm=True)。”*
3. **阶段 3：活跃消费组状态冲突防御**  
   若消费组当前存在在线活跃消费者实例（`state="Stable"` 且成员数 $>0$），直接重置位移将被在线客户端后续心跳与提交覆盖。工具默认会拦截并返回防御警示；若确认属于应急处置，可显式追加 `force=True` 强制放行。

---

## ⚙️ 运行时配置与参数清单

服务配置优先遵循：**命令行参数 > 环境变量 > 预设默认值**。

| 命令行参数 | 对应环境变量 | 默认值 | 详细说明 |
| :--- | :--- | :--- | :--- |
| `-c, --config` | `MCP_KAFKA_CONFIG` | 无 | 多集群命名连接 YAML 配置文件路径（指定后启用多集群模式） |
| `--bootstrap-servers` | `MCP_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | 单集群模式 Broker 引导地址（未指定配置文件时生效，多个逗号分隔） |
| `--read-only` | `MCP_KAFKA_READ_ONLY` | `false` | 全局只读防线开关，开启后拒绝一切写操作；位移重置锁定仅允许 `dry_run=True` 预检 |
| `--security-protocol` | `MCP_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | 底层通信安全协议 (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`) |
| `--sasl-mechanism` | `MCP_KAFKA_SASL_MECHANISM` | 无 | SASL 认证机制 (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` 等) |
| `--sasl-username` | `MCP_KAFKA_SASL_USERNAME` | 无 | SASL 身份认证用户名 / AccessKey |
| `--sasl-password` | `MCP_KAFKA_SASL_PASSWORD` | 无 | SASL 身份认证密码 / SecretKey (支持 `${VAR:-default}` 动态插值) |
| `--ssl-cafile` | `MCP_KAFKA_SSL_CAFILE` | 无 | SSL 根证书 CA 路径 (`.pem` / `.crt`)，严格 Fail-Fast 校验存在性 |
| `--ssl-certfile` | `MCP_KAFKA_SSL_CERTFILE` | 无 | mTLS 双向认证客户端证书路径 (须与私钥成对提供) |
| `--ssl-keyfile` | `MCP_KAFKA_SSL_KEYFILE` | 无 | mTLS 双向认证客户端私钥路径 (须与证书成对提供) |
| `--transport` | `MCP_KAFKA_TRANSPORT` | `stdio` | 传输协议网关类型：`stdio`（管道）或 `sse`（HTTP） |
| `--host` | `MCP_KAFKA_SERVER_HOST` | `0.0.0.0` | HTTP SSE 模式监听主机地址 |
| `--port` | `MCP_KAFKA_SERVER_PORT` | `8000` | HTTP SSE 模式监听端口号 |
| `--log-level` | `MCP_KAFKA_LOG_LEVEL` | `INFO` | 服务日志级别 (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |


### 🔐 企业级安全认证协议指南 (SASL / SSL / mTLS)

服务端对企业私有云与公有云托管 Kafka 提供了完备的安全认证矩阵支持：

1. **协议支持矩阵**：
   - **`PLAINTEXT`**：内网非加密通信（默认）；
   - **`SASL_SSL` (主流云厂商标准)**：传输层 TLS 加密 + 身份认证，广泛应用于阿里云云消息队列 Kafka、AWS MSK、华为云及 Confluent Cloud；
   - **`SASL_PLAINTEXT`**：内网环境仅身份认证、明文数据传输；
   - **`SSL` (mTLS 双向证书)**：金融与高保密专网环境下，基于客户端与服务端双向数字证书完成认证。
2. **凭据安全与环境变量插值最佳实践**：
   为防止在版本控制代码库（如 Git）或本地 JSON 配置中硬编码敏感账密，服务端内建了 `${ENV_VAR}` 动态插值解析引擎。无论在 CLI 还是 `connections.yaml` 中，均建议使用环境变量注入：
   ```bash
   export KAFKA_SASL_USER="my_access_key"
   export KAFKA_SASL_PASS="my_secret_token"
   ```

---

## 🚀 极速安装与通用接入指南

本项目已正式发布至 PyPI，包名为 [`atengk-mcp-server-kafka`](https://pypi.org/project/atengk-mcp-server-kafka/)。可直接通过 `uvx` 免安装一键调用，或通过 `pip` 安装至现有 Python 环境。

### 1. 运行与安装方式

- **方式 A：使用 `uvx` 免安装开箱即用 (推荐)**
  无需手动克隆代码或配置虚拟环境，只需系统安装了 [uv](https://docs.astral.sh/uv/) 工具链：
  ```bash
  uvx atengk-mcp-server-kafka --bootstrap-servers localhost:9092
  ```

- **方式 B：通过 `pip` 安装到 Python 环境**
  ```bash
  pip install atengk-mcp-server-kafka

  # 安装后系统直接提供可执行命令
  mcp-server-kafka --bootstrap-servers localhost:9092
  ```

---

### 2. MCP 客户端通用配置 (通用标准模板)

以下配置遵循标准 [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) 规范，**通用支持任意 MCP 宿主环境（如 Claude Desktop、Cursor、VS Code MCP 插件、Windsurf 等各类 AI Agent 客户端）**。

您可以根据宿主偏好选择通过 **CLI 命令行参数 (`args`)** 或 **环境变量 (`env`)** 注入配置：

#### 方案 A：命令行参数模式 (`args`)

通过 `args` 数组显式传递服务启动参数：

```json
{
  "mcpServers": {
    "kafka": {
      "command": "uvx",
      "args": [
        "atengk-mcp-server-kafka",
        "--bootstrap-servers",
        "localhost:9092",
        "--read-only"
      ]
    }
  }
}
```

> **提示**：若已通过 `pip install atengk-mcp-server-kafka` 全局安装，可将 `"command"` 设为 `"mcp-server-kafka"`，并在 `"args"` 中直接传参：`["--bootstrap-servers", "localhost:9092", "--read-only"]`。

#### 方案 B：环境变量模式 (`env`)

通过 `env` 对象声明环境变量，服务端启动时将自动加载对应配置项：

```json
{
  "mcpServers": {
    "kafka": {
      "command": "uvx",
      "args": [
        "atengk-mcp-server-kafka"
      ],
      "env": {
        "MCP_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "MCP_KAFKA_READ_ONLY": "true",
        "MCP_KAFKA_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

#### 方案 C：HTTP SSE 远程常驻服务模式 (`url`)

若服务端部署在远端服务器或 Docker 容器内以 SSE 网关模式常驻运行，客户端直接配置服务监听 URL：

```json
{
  "mcpServers": {
    "kafka": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

#### 方案 D：多集群/多环境配置模式 (`--config connections.yaml`)

> [!TIP]
> **极速上手多集群**：在项目根目录或配置目录下执行 `cp connections.example.yaml connections.yaml`，填入各集群真实 Broker 引导地址后，将该文件路径传入 `--config` 即可无缝切换多环境。

当需要同时纳管本地开发、联调测试与生产只读等多套物理隔离的 Kafka 集群时，可基于 [`connections.example.yaml`](./connections.example.yaml) 模板定义连接字典，并通过 `--config` 传入：

```json
{
  "mcpServers": {
    "kafka": {
      "command": "uvx",
      "args": [
        "atengk-mcp-server-kafka",
        "--config",
        "/绝对路径/connections.yaml"
      ]
    }
  }
}
```

> **企业级多环境配置文件模板 (`connections.yaml`)**：
> ```yaml
> default_connection: "default"
> read_only: false
> 
> connections:
>   # 1. 本地开发环境：单机无密通信
>   default:
>     bootstrap_servers: "localhost:9092"
>     read_only: false
> 
>   # 2. 云端托管集群：SASL_SSL 加密认证 (环境变量动态插值，防凭据泄露)
>   cloud_sasl:
>     bootstrap_servers: "alikafka-pre-cn-xxxx.kafka.aliyuncs.com:9093"
>     security_protocol: "SASL_SSL"
>     sasl_mechanism: "SCRAM-SHA-256"
>     sasl_username: "${KAFKA_SASL_USER}"
>     sasl_password: "${KAFKA_SASL_PASSWORD}"
>     ssl_cafile: "/etc/ssl/certs/kafka-ca.pem"
>     read_only: false
> 
>   # 3. 核心生产集群：mTLS 双向证书通信 + 细粒度只读写保护
>   production_mtls:
>     bootstrap_servers: "kafka-prod-1.internal:9093,kafka-prod-2.internal:9093"
>     security_protocol: "SSL"
>     ssl_cafile: "./certs/ca.pem"
>     ssl_certfile: "./certs/client.cer"
>     ssl_keyfile: "./certs/client.key"
>     read_only: true  # 生产实例强制连接级写保护
> ```
> 此时所有工具均支持可选传入 `connection: "cloud_sasl"` 或 `connection: "production_mtls"`。若未传参则自动使用 `default_connection`。可调用 `kafka_list_connections` 查询当前所有已连接的集群清单及其安全协议。

---

### 3. Docker 容器常驻网关 (HTTP SSE 模式)

适合生产环境部署、团队共享公共网关或隔离运行环境：

```bash
# 方式 A：docker run 快速启动（连接宿主机 Kafka 并开启只读防线）
docker run -d \
  --name mcp-server-kafka \
  -p 8000:8000 \
  -e MCP_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -e MCP_KAFKA_READ_ONLY=true \
  ghcr.io/atengk/mcp-server-kafka:latest

# 方式 B：Docker Compose 一键启动
docker compose up -d
```

服务就绪后，可快速检验 SSE 监听端点连通性：
```bash
curl -I http://localhost:8000/sse
```

---

### 4. 常见连接场景速查 (Recipes)

- **场景 1：连接本机单机 Kafka（宿主机原生运行）**
  ```bash
  --bootstrap-servers localhost:9092
  ```
- **场景 2：连接 Docker 内运行的 Kafka（容器与宿主机互通）**
  - 本地宿主机客户端：`localhost:9092`
  - Docker 容器内运行 MCP：`host.docker.internal:9092`
- **场景 3：连接远程/生产环境多 Broker 高可用集群并启用只读安全防护**
  ```bash
  --bootstrap-servers 10.0.1.11:9092,10.0.1.12:9092,10.0.1.13:9092 --read-only
  ```
- **场景 4：多集群/多环境同时连接（Dev / Staging / Prod 并存）**
  - **方式 A（多连接配置模式，推荐）**：启动单进程并通过 `--config connections.yaml` 挂载多集群，在对话中直接告知 AI：“查看 staging 集群的主题列表，并排查 production 集群的消费积压”；
  - **方式 B（多 Server 实例模式）**：在 MCP 配置中配置多个命名服务（如 `"kafka-dev"` 与 `"kafka-prod"`），分别传入各自的连接地址与参数。
- **场景 5：连接公有云/云托管 Kafka（阿里云/AWS MSK/华为云，SASL_SSL + SCRAM）**
  ```bash
  uvx atengk-mcp-server-kafka \
    --bootstrap-servers "alikafka-pre-cn-xxxx.kafka.aliyuncs.com:9093" \
    --security-protocol SASL_SSL \
    --sasl-mechanism SCRAM-SHA-256 \
    --sasl-username "$KAFKA_USER" \
    --sasl-password "$KAFKA_PASSWORD"
  ```
- **场景 6：连接内网双向证书认证集群 (mTLS / SSL)**
  ```bash
  uvx atengk-mcp-server-kafka \
    --bootstrap-servers "kafka-broker-1:9093" \
    --security-protocol SSL \
    --ssl-cafile "./ca.pem" \
    --ssl-certfile "./client.cer" \
    --ssl-keyfile "./client.key"
  ```

---

### 5. ❓ 常见问题与排障 (FAQ)

> **Q1：提示 `command not found: uvx` 或找不到执行程序？**  
> **A**：说明客户端桌面环境未继承包含 `uv` 的系统环境变量 `PATH`。可将 `"command"` 替换为系统上 `uvx` 的绝对路径（Windows 如 `"C:\\Users\\<用户名>\\.cargo\\bin\\uvx.exe"`，macOS/Linux 如 `"/Users/<用户名>/.cargo/bin/uvx"`），或通过 `pip install atengk-mcp-server-kafka` 后将 command 设为可执行文件的绝对路径。

> **Q2：连接报错 `ConnectionRefusedError` 连不上 Kafka？**  
> **A**：请检查 Kafka 运行状态与端口监听；若 Kafka 位于 Docker 容器内而 MCP 服务端位于另一容器或本地，注意使用 `host.docker.internal` 或挂载于同一 Docker 网络网桥。

> **Q3：为什么工具列表中没有主题创建、删除或消息发送工具？**  
> **A**：这是服务端的**全局只读安全防线**。当开启了 `--read-only` 或设置了 `MCP_KAFKA_READ_ONLY=true` 时，所有破坏性与写操作工具将自动隐藏并拒绝执行，确保生产集群安全。若确需写权限，移除该参数重新启动即可。

> **Q4：调用 `kafka_reset_consumer_group_offsets` 报错消费组处于活跃状态冲突？**  
> **A**：这是服务端的**活跃组防并发覆盖防线**。当消费组处于 `Stable` 状态且成员数 $>0$ 时，在线客户端会定时提交位移，覆盖重置结果。最佳实践是先停止消费端实例；若属于紧急运维且确认要强行覆盖，可在确认后传入 `force=True` 放行。

> **Q5：如何在多环境配置中防范 SASL 账密明文泄露？**  
> **A**：服务端支持 `${ENV_VAR}` 语法动态插值。在 `connections.yaml` 中将敏感字段写作 `sasl_username: "${KAFKA_USER}"` 与 `sasl_password: "${KAFKA_PASSWORD}"`，启动前在宿主系统中 export 对应环境变量即可，杜绝明文入库。

---

### 6. 🛠️ 源码构建与参与贡献

若需针对源码开展二次开发或本地贡献调试：

```bash
# 克隆源码并同步全量开发依赖
git clone https://github.com/atengk/mcp-server-kafka.git
cd mcp-server-kafka
uv sync --all-extras

# 运行全量单元测试与 Lint 门禁
uv run pytest
uv run ruff check .
```

---

## 📂 仓库目录结构

```text
.
├── .github/                      # GitHub Actions CI/CD 流水线与 Issue/PR 模版
│   ├── ISSUE_TEMPLATE/
│   ├── workflows/
│   │   ├── ci.yml                # 自动化测试与 Lint 门禁
│   │   └── release.yml           # 自动化发版与多架构 GHCR 镜像构建
│   └── PULL_REQUEST_TEMPLATE.md
├── docs/                         # 工程架构决策与领域文档
│   ├── adr/                      # 架构决策记录 (ADR 0001 ~ 0005)
│   └── spec/                     # 核心需求规格说明书
├── src/                          # 核心源码目录
│   └── mcp_server_kafka/
│       ├── __init__.py           # 版本号与包声明
│       ├── config.py             # 配置模型与多连接参数解析
│       ├── models.py             # 领域数据模型 (Pydantic)
│       ├── manager.py            # aiokafka 异步客户端生命周期与多连接注册中心
│       └── server.py             # FastMCP 服务装配、Tools/Resources/Prompts 路由
├── tests/                        # 最高测试接缝集成测试套件
│   ├── test_basic.py             # 基础发版与版本号测试
│   ├── test_cluster_tools.py     # 集群元数据与资源测试
│   ├── test_config.py            # 配置加载与合并解析测试
│   ├── test_consumer_group_tools.py # 消费组与 Lag 诊断测试
│   ├── test_message_produce.py   # 消息安全生产与序列化测试
│   ├── test_message_sampling.py  # 零位移消息采样与自适应解码测试
│   ├── test_multi_cluster_connections.py # 多集群命名连接与路由测试
│   ├── test_security_and_offset_reset.py # 企业级安全与位移重置治理测试
│   ├── test_topic_tools.py       # 主题生命周期与安全防线测试
│   └── test_transport_and_gateway.py # Stdio/SSE 双模网关调度测试
├── connections.example.yaml      # 多集群多环境连接配置文件模板
├── CONTEXT.md                    # 统一领域术语表
├── docker-compose.yml            # 生产常驻 SSE 编排
├── Dockerfile                    # 生产级多阶段容器镜像构建文件
├── pyproject.toml                # 项目元数据与依赖定义 (PEP 621)
└── README.md                     # 项目使用指南与能力文档
```

---

## 📄 开源许可证

本项目基于 [Apache License 2.0](./LICENSE) 协议开源。
