# mcp-server-kafka

<p align="center">
  <strong>基于 Python 与 FastMCP 构建的生产级 Apache Kafka 模型上下文协议服务端</strong>
</p>

<p align="center">
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
- 🛡️ **双重安全防线**：
  - **一级防线（全局只读）**：开启 `--read-only` 模式时，一切变更类工具自动隐藏并阻断执行；
  - **二级防线（高危确认）**：删除主题等破坏性操作强制要求显式传入 `confirm=True` 二次确认；
- 🔍 **零位移侵入瞬态采样**：采样读取消息时不加入消费组、强制不向 `__consumer_offsets` 提交位移，绝不破坏生产环境业务消费进度；
- 🧩 **自适应内容解析**：消息载荷优先解析为结构化 JSON 或 UTF-8 文本，二进制载荷安全降级为 Base64；
- 🌐 **Stdio / HTTP SSE 双模传输**：完美兼顾本地单机桌面客户端（如 Claude Desktop）与云原生容器常驻运行；
- 🚀 **开箱即用容器化**：提供精简多阶段 Dockerfile（无 root 用户 `appuser`）与 docker-compose 编排。

---

## 🛠️ MCP 协议能力清单 (Tools, Resources, Prompts)

### 1. Tools 工具集 (9 项)

| 工具名称 | 功能描述 | 核心入参 | 安全策略 |
| :--- | :--- | :--- | :--- |
| `kafka_cluster_info` | 查询 Kafka 集群元数据与 Broker 节点列表 | 无 | 只读安全 |
| `kafka_list_topics` | 列出集群主题清单及分区数 | `pattern`（可选模糊过滤）, `include_internal` | 只读安全 |
| `kafka_describe_topic` | 查询指定主题的分区分布、Leader 节点、ISR 与自定义配置 | `topic_name` | 只读安全 |
| `kafka_create_topic` | 创建新主题 | `topic_name`, `partitions`, `replication_factor` | 只读模式下自动隐藏 |
| `kafka_delete_topic` | 删除指定主题 | `topic_name`, `confirm: bool = False` | 强制 `confirm=True` 二次确认；只读模式下隐藏 |
| `kafka_produce_message` | 向指定主题发送消息（支持字典/列表 JSON、文本与 Headers） | `topic`, `value`, `key`, `partition`, `headers` | 只读模式下自动隐藏 |
| `kafka_sample_messages` | 零提交位移瞬态采样读取消息，自适应格式解码 | `topic`, `partition`, `strategy`, `offset`, `limit` | 只读安全 (上限 100 条) |
| `kafka_list_consumer_groups` | 枚举集群中所有消费组 ID、协议类型与运行状态 | 无 | 只读安全 |
| `kafka_describe_consumer_group` | 查询消费组各分区 Committed Offset、LEO、Lag 及活跃成员分配 | `group_id` | 只读安全 (含已删除主题防御) |

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

## ⚙️ 运行时配置与参数清单

服务配置优先遵循：**命令行参数 > 环境变量 > 预设默认值**。

| 命令行参数 | 对应环境变量 | 默认值 | 详细说明 |
| :--- | :--- | :--- | :--- |
| `--bootstrap-servers` | `MCP_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka 集群 Broker 引导地址（多个以逗号分隔） |
| `--read-only` | `MCP_KAFKA_READ_ONLY` | `false` | 全局只读防线开关，开启后拒绝并隐藏一切写操作 |
| `--transport` | `MCP_KAFKA_TRANSPORT` | `stdio` | 传输协议网关类型：`stdio`（管道）或 `sse`（HTTP） |
| `--host` | `MCP_KAFKA_SERVER_HOST` | `0.0.0.0` | HTTP SSE 模式监听主机地址 |
| `--port` | `MCP_KAFKA_SERVER_PORT` | `8000` | HTTP SSE 模式监听端口号 |
| `--log-level` | `MCP_KAFKA_LOG_LEVEL` | `INFO` | 服务日志级别 (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

---

## 🚀 接入与部署指南

### 1. 本地安装与开发

推荐使用极速包管理器 [uv](https://docs.astral.sh/uv/)：

```bash
# 克隆仓库
git clone https://github.com/atengk/mcp-server-kafka.git
cd mcp-server-kafka

# 创建虚拟环境并同步全量依赖
uv sync --all-extras

# 运行自动化测试与代码规范检查
uv run pytest
uv run ruff check .
```

### 2. 客户端集成示例 (以 Claude Desktop 为例)

编辑 Claude Desktop 配置文件 `claude_desktop_config.json`：

#### 方案 A：通过 Stdio 标准管道连接（默认本地模式）

```json
{
  "mcpServers": {
    "kafka": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/mcp-server-kafka",
        "run",
        "mcp-server-kafka",
        "--bootstrap-servers",
        "localhost:9092",
        "--read-only"
      ]
    }
  }
}
```

#### 方案 B：连接常驻 HTTP SSE 服务网关

若服务端已在容器或远端以 SSE 模式运行，客户端可直接配置 SSE URL：

```json
{
  "mcpServers": {
    "kafka": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

### 3. Docker 与 Docker Compose 生产运行 (常驻 SSE 网关模式)

本项目镜像发布于 GitHub Container Registry (GHCR)：

```bash
# 方式 A：使用 Docker Compose 一键启动常驻服务
docker compose up -d

# 方式 B：使用 docker run 启动
docker run -d \
  --name mcp-server-kafka \
  -p 8000:8000 \
  -e MCP_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -e MCP_KAFKA_READ_ONLY=true \
  ghcr.io/atengk/mcp-server-kafka:latest
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
│   ├── adr/                      # 架构决策记录 (ADR 0001 ~ 0003)
│   └── spec/                     # 核心需求规格说明书
├── src/                          # 核心源码目录
│   └── mcp_server_kafka/
│       ├── __init__.py           # 版本号与包声明
│       ├── config.py             # 配置模型与 CLI 参数解析
│       ├── models.py             # 领域数据模型 (Pydantic)
│       ├── manager.py            # aiokafka 纯异步客户端连接与运维生命周期管理
│       └── server.py             # FastMCP 服务装配、Tools/Resources/Prompts 路由
├── tests/                        # 最高测试接缝集成测试套件
│   ├── test_basic.py             # 基础发版与版本号测试
│   ├── test_cluster_tools.py     # 集群元数据与资源测试
│   ├── test_config.py            # 配置加载与合并解析测试
│   ├── test_consumer_group_tools.py # 消费组与 Lag 诊断测试
│   ├── test_message_produce.py   # 消息安全生产与序列化测试
│   ├── test_message_sampling.py  # 零位移消息采样与自适应解码测试
│   ├── test_topic_tools.py       # 主题生命周期与安全防线测试
│   └── test_transport_and_gateway.py # Stdio/SSE 双模网关调度测试
├── CONTEXT.md                    # 统一领域术语表
├── docker-compose.yml            # 生产常驻 SSE 编排
├── Dockerfile                    # 生产级多阶段容器镜像构建文件
├── pyproject.toml                # 项目元数据与依赖定义 (PEP 621)
└── README.md                     # 项目使用指南与能力文档
```

---

## 📄 开源许可证

本项目基于 [Apache License 2.0](./LICENSE) 协议开源。
