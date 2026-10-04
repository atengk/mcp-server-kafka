# mcp-server-kafka

<p align="center">
  <strong>基于 Python 开发的 Apache Kafka MCP (Model Context Protocol) 服务端</strong>
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

`mcp-server-kafka` 是一个遵循 [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) 规范的 Apache Kafka 服务端集成。通过标准化的 MCP 协议，让大型语言模型（LLM）与 AI Agent 能够安全、高效地与 Apache Kafka 消息集群交互。

本项目工程底座参考 [oss-template](https://github.com/atengk/oss-template) 规范构建，提供开箱即用的现代化 CI/CD 流水线与自动化发版机制。

---

## ✨ 核心特性

- 🎯 **现代 Python 技术栈**：基于 Python 3.10+ 与 `uv` 构建，遵循 PEP 621 标准规范；
- ⚡ **纯异步高性能架构**：底层基于 `aiokafka` 纯异步事件驱动，天然契合 MCP 协议；
- 🚀 **自动化发版流水线**：打 Tag（如 `v0.1.0`）自动触发发版、自动提取 PR/Commit 生成精美更新日志、自动挂载打包物附件；
- 🛡️ **规范化工作流**：支持 Conventional Commits 提交规范、预置结构化 Issue 反馈与标准 PR 审查模版。

---

## 🛠️ 快速开始

### 1. 环境准备

本项目推荐使用现代极速包管理器 [uv](https://docs.astral.sh/uv/)：

```bash
# 克隆仓库
git clone https://github.com/atengk/mcp-server-kafka.git
cd mcp-server-kafka

# 创建虚拟环境并同步依赖
uv sync --all-extras
```

### 2. 运行测试与代码检查

```bash
# 执行单元测试
uv run pytest

# 执行代码风格与语法检查
uv run ruff check .
```

### 3. MCP 客户端配置示例

以 Claude Desktop (`claude_desktop_config.json`) 为例：

```json
{
  "mcpServers": {
    "kafka": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/mcp-server-kafka",
        "run",
        "mcp-server-kafka"
      ],
      "env": {
        "MCP_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"
      }
    }
  }
}
```

### 4. Docker 与 Docker Compose 运行 (常驻 SSE 网关模式)

本项目已集成多架构 Docker 镜像，并自动发布至 GitHub Container Registry (GHCR)：

```bash
# 使用 Docker Compose 一键启动常驻服务
docker compose up -d

# 或使用 docker run 直接启动
docker run -d \
  --name mcp-server-kafka \
  -p 8000:8000 \
  -e MCP_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  ghcr.io/atengk/mcp-server-kafka:latest
```

---

## 🚀 版本发版指南

当准备发布新版本时，推送一个语义化版本号的 Git Tag 即可触发自动化发版：

```bash
# 1. 确保本地 main 分支代码最新且 CI 绿灯通过
git checkout main
git pull origin main

# 2. 打标签并推送到 GitHub (支持 v0.1.0, v1.0.0 等)
git tag v0.1.0
git push origin v0.1.0
```

GitHub Actions 将会自动执行 [`.github/workflows/release.yml`](./.github/workflows/release.yml)：
1. 提取自上一版本以来的全部合并 PR 与提交记录；
2. 自动生成 GitHub Release 详情并归类贡献者；
3. 将打包产物与 SHA-256 校验和自动挂载至 Release 页面附件；
4. 自动构建多架构 Docker 镜像并推送至 GHCR (`ghcr.io/atengk/mcp-server-kafka`)。

---

## 📂 仓库目录结构

```text
.
├── .github/
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.md
│   │   └── feature_request.md
│   ├── workflows/
│   │   ├── ci.yml
│   │   └── release.yml
│   └── PULL_REQUEST_TEMPLATE.md
├── src/
│   └── mcp_server_kafka/
│       ├── __init__.py
│       └── server.py
├── tests/
│   ├── __init__.py
│   └── test_basic.py
├── .cliff.toml
├── .dockerignore
├── .editorconfig
├── .gitattributes
├── .gitignore
├── CONTRIBUTING.md
├── docker-compose.yml
├── Dockerfile
├── LICENSE
├── README.md
└── pyproject.toml
```

---

## 📄 开源许可证

本项目基于 [Apache License 2.0](./LICENSE) 协议开源。
