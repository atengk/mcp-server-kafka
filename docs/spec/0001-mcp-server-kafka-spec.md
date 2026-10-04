# 需求规格说明书：mcp-server-kafka 核心功能与架构

> **交付与验收状态 (Status)**: ✅ 100% Completed & Verified (全部 6 项子工单已闭环并全量合并至 `main` 分支)
>
> | 工单编号 | 任务标题 | 交付提交 | 状态 |
> | :--- | :--- | :--- | :---: |
> | #2 | feat(cluster): 集群元数据只读接口与基础运行框架 | `68c8372` | **CLOSED** |
> | #3 | feat(topic): 主题生命周期管理与安全防线机制 | `cc0ece4` | **CLOSED** |
> | #4 | feat(producer): 消息安全生产通道与多类型载荷支持 | `19e55cb` | **CLOSED** |
> | #5 | feat(consumer): 瞬态无侵入消息采样与自适应解码 | `f006c2e` | **CLOSED** |
> | #6 | feat(group): 消费组状态监测与积压 (Lag) 根因诊断 | `9a13b5d` | **CLOSED** |
> | #7 | feat(transport): Stdio 与 HTTP SSE 双模传输网关及生产容器集成 | `ea3922e` | **CLOSED** |

## 问题陈述 (Problem Statement)

当大语言模型（LLM）与 AI Agent 需要与企业级 Apache Kafka 消息集群交互时，面临以下核心痛点：
1. **缺乏标准化上下文协议交互界面**：缺乏对 MCP (Model Context Protocol) 规范的完善支持，模型无法直观枚举集群元数据、操作主题与排查消息；
2. **生产安全风险极高**：AI 智能体直接调用 Kafka 驱动可能触发误删核心主题、误清空数据等毁灭性灾难；
3. **排查探查破坏业务消费进度**：常规 Kafka 消费者会加入消费组并提交位移，模型单纯为了诊断问题而读取消息会直接破坏下游生产消费者的实际业务进度；
4. **消息格式多变难解析**：Kafka 消息体涵盖 JSON、纯文本、Avro/Protobuf 或原生二进制，传统排查难以自适应解码呈现；
5. **部署通信形态单一**：无法灵活兼顾本地桌面单机客户端（Stdio 管道交互）与云原生容器化常驻（HTTP SSE）运行场景。

## 解决方案 (Solution)

构建基于 Python 与 FastMCP 的生产级 Apache Kafka 模型上下文协议服务端 (`mcp-server-kafka`)：
1. **全景功能覆盖**：全面提供集群元数据只读、主题生命周期管理、消息安全生产、无侵入消息采样读取以及消费组积压 (Lag) 诊断；
2. **双重安全防线**：支持全局只读开关 (`--read-only`)，并在常规模式下对删除主题等高危破坏动作强制要求 `confirm=True` 二次确认；
3. **零侵入瞬态消息采样 (Ephemeral Fetch)**：关闭自动提交位移，不接入生产业务消费组，支持按指定位移、最新或最旧安全探查消息；
4. **自适应内容解码**：智能解析 JSON 与 UTF-8 文本，二进制安全降级 Base64 并随附完整消息元数据；
5. **协议三要素与双模传输**：全面提供 Tools、Resources、Prompts 协同，并支持 Stdio 与 HTTP SSE 双模通信。

## 用户故事清单 (User Stories)

1. 作为 AI Agent，我想查看 Kafka 集群元数据与 Broker 节点列表，以便全面掌握消息集群的健康状态与拓扑结构。
2. 作为 AI Agent，我想列出集群中的所有主题 (Topics) 并查看其分区数与配置，以便选择正确的数据通道。
3. 作为 AI Agent，我想查询指定主题的详细信息（分区分布、Leader 节点、ISR 副本列表与配置项），以便诊断主题健康度。
4. 作为 AI Agent，我想创建新的 Kafka 主题并指定分区数与副本因子，以便为新数据流开辟存储通道。
5. 作为 AI Agent，当删除 Kafka 主题时若未传入二次确认标志 (`confirm=True`)，系统应当予以拦截并拒绝执行，以便防范误操作导致不可逆的数据丢失。
6. 作为 AI Agent，我想在显式提供二次确认标志的前提下删除指定主题，以便完成测试数据清理或废弃管道回收。
7. 作为 AI Agent，我想向指定主题或特定分区生产消息（支持消息键 Key、消息载荷 Value 以及可选 Headers），以便向消息流注入业务事件或测试数据。
8. 作为 AI Agent，我想从指定主题以零提交位移的方式只读采样拉取最新或特定位移的消息，以便探查数据内容且绝不干扰线上真实消费者的消费进度。
9. 作为 AI Agent，当采样拉取包含 JSON、纯文本或二进制数据时，我希望系统自适应解析并输出元数据，以便清晰理解不同编码载荷的内容。
10. 作为 AI Agent，我想列出集群内所有活跃的消费组 (Consumer Groups)，以便排查有哪些业务方正在订阅数据。
11. 作为 AI Agent，我想查询特定消费组在各分区的已提交位移与积压量 (Lag)，以便快速定位下游消费迟滞与系统瓶颈。
12. 作为运维工程师，我希望能够在启动时开启全局只读模式 (`--read-only` 或 `MCP_KAFKA_READ_ONLY=true`)，以便彻底禁止 AI Agent 执行任何主题创建、删除或消息写入动作。
13. 作为开发者，我希望 MCP 服务端支持 Stdio 管道与 HTTP SSE 两种传输协议，以便既能用于桌面客户端（如 Claude Desktop），也能容器化部署常驻运行。
14. 作为 AI Agent，我希望通过 MCP Resources 读取动态集群概况与主题元数据，以便在会话中直接挂载只读上下文。
15. 作为 AI Agent，我希望调用预置的 Prompts 模板（如消费积压根因诊断），以便快速生成标准的运维排查指引与分析方案。
16. 作为开发者与运维工程师，我希望能够通过 YAML 配置文件 (`--config connections.yaml`) 统一声明并纳管多套物理隔离的 Kafka 集群（如 default、staging、production），并在工具调用时按需路由或对生产连接实施细粒度只读保护，以便在单一会话中无缝跨环境协同运维。

## 实现决策 (Implementation Decisions)

- **框架与构建后端**：基于 Python 3.10+、FastMCP 框架与 `uv` 构建，采用 `hatchling` 构建后端，严格遵循 PEP 621 标准。
- **底层驱动架构**：消息生产、消息消费及基础通信基于 `aiokafka` 纯异步驱动，保障高并发无阻塞；管理类操作（Topic CRUD、消费组与 Lag 计算）封装为异步 Admin 接口协同调度。
- **协议能力设计**：
  - **Tools 列表 (10 项，全量支持可选 `connection` 动态路由参数)**：
    - `kafka_list_connections`：枚举当前服务端已配置的所有 Kafka 集群命名连接清单及只读状态；
    - `kafka_cluster_info`：获取集群概况与 Broker 节点列表；
    - `kafka_list_topics`：列出所有主题及其分区基本信息；
    - `kafka_describe_topic`：查询指定主题的详细分区、副本与配置；
    - `kafka_create_topic`：创建新主题（支持指定 partitions 与 replication_factor，只读模式或只读连接下拦截）；
    - `kafka_delete_topic`：删除主题（强制 `confirm: bool = False` 防御，只读模式或只读连接下拦截）；
    - `kafka_produce_message`：向主题发送消息（支持 key、value、headers，只读模式或只读连接下拦截）；
    - `kafka_sample_messages`：零位移提交只读采样消息（支持 topic、partition、offset/strategy、limit）；
    - `kafka_list_consumer_groups`：列出集群所有消费组；
    - `kafka_describe_consumer_group`：查询消费组详情及各分区的 Committed Offset 与 Lag。
  - **Resources 列表**：
    - `kafka://cluster/summary`：集群整体元数据与 Broker 清单；
    - `kafka://topics/{topic}`：指定主题的实时状态与分区明细。
  - **Prompts 列表**：
    - `diagnose_topic_lag`：消费组积压排查与性能瓶颈诊断提示词模板；
    - `inspect_topic_messages`：主题消息数据采样探查与数据清洗分析模板。
- **安全与防护层**：
  - 双重安全防线：环境变量 `MCP_KAFKA_READ_ONLY` 或命令行 `--read-only` 开启时，写操作 Tools 自动隐藏并拒绝执行；`delete_topic` 强制参数防御。
  - 认证协议支持：支持 `PLAINTEXT`、`SASL_PLAINTEXT` 与 `SASL_SSL` (PLAIN、SCRAM-SHA-256、SCRAM-SHA-512)，凭据与敏感字段输出脱敏。
- **双模传输网关**：
  - Stdio 传输：通过标准输入输出与外部宿主交互；
  - SSE 传输：通过 Starlette/FastAPI/Uvicorn HTTP 挂载常驻服务，默认监听 `0.0.0.0:8000`。

## 测试决策 (Testing Decisions)

- **最高测试接缝 (Top Test Seam)**：统一采用 **FastMCP 服务接口层 / 协议调用层 (In-memory FastMCP Client Session / Tool Invocation Interface)** 作为单一最高测试接缝。
- **良好测试标准**：
  - 测试仅聚焦于外部可观测行为与契约输出（如输入参数校验、成功返回内容格式、只读模式拦截报错、`confirm` 防御逻辑）；
  - 严禁测试内部私有实现细节；
  - 核心底层 Kafka 驱动通过轻量异步测试桩 (Async Test Fakes/Mocks) 隔离，确保无外部 Kafka 环境下也能实现毫秒级自动化单元与集成测试。
- **测试覆盖模块**：
  - 元数据工具测试 (`test_cluster_tools.py`)；
  - 主题管理与安全防护测试 (`test_topic_tools.py`，重点验证只读模式拦截与 `confirm` 校验)；
  - 消息收发与自适应解码测试 (`test_message_tools.py`，覆盖 JSON、纯文本、二进制等多种场景)；
  - 消费组与 Lag 诊断测试 (`test_consumer_group_tools.py`)；
  - Resources 与 Prompts 契约测试 (`test_resources_prompts.py`)。

## 范围外明确界定 (Out of Scope)

- Kafka Connect / Schema Registry / ksqlDB 的专用管理接口不在本期范围内；
- Kafka Broker 级别的底层物理配置动态热修改（如滚动重启、动态更新 broker 配置文件）不在本期范围内；
- 生产消息事务 (Transactions / Exactly-Once Semantics) 的高级两阶段提交编排不在本期范围内。

## 补充说明 (Further Notes)

- 本规格文档已由 `/grill-with-docs` 会话完整对齐，架构决策已沉淀于 `docs/adr/0001`、`docs/adr/0002` 与 `docs/adr/0003`，领域术语表统一遵循根目录 `CONTEXT.md`；
- 全部 6 个垂直切片子任务已按计划通过 TDD 闭环落地并全量通过 34 项最高测试接缝自动化测试，本规格说明书已完成全流程验收并正式归档。

