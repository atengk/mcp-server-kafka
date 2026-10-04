# 0007: GitHub Container Registry 容器镜像标签矩阵与发版流水线对齐规范

为了确保发布至 GitHub Container Registry (ghcr.io) 的多架构 Docker 镜像具备符合工业标准的语义化版本标签体系与 `latest` 标签自动管理能力，我们决定全面参考并对齐 `atengk/oss-template` 的标准发布配置。

## 决策背景

1. **缺失 `latest` 标签导致拉取阻断**：
   此前流水线生成的 Docker 镜像在特定情况下缺少 `latest` 标签，导致使用 `docker run ghcr.io/atengk/mcp-server-kafka:latest` 的终端用户与生产编排（如 Docker Compose、Kubernetes）拉取镜像失败。
2. **浅检出截断 Git 历史致使 SemVer 推导异常**：
   此前 `publish-docker` Job 中的 `actions/checkout@v4` 采用了默认的浅检出（`fetch-depth: 1`），使 `docker/metadata-action` 无法获知仓库完整的历史标签拓扑，从而无法精确计算当前构建是否为最新的正式语义化版本。
3. **缺少大版本滚动标签**：
   缺少形如 `:1` 的 Major 版本标签，使得下游用户无法在保持次版本向前兼容的前提下平滑接收补丁更新。

## 决策内容

1. **全量源码与标签树检出 (Full Git History)**：
   在 `publish-docker` Job 的检出步骤中显式指定 `fetch-depth: 0`，完整同步仓库全量 Git Commit 与 Tag 历史，为语义化版本推导与 `latest` 计算提供底层数据支撑。

2. **三级语义化版本矩阵与 `latest` 自动化管控**：
   废弃原本脆弱的硬编码 `type=raw,value=latest`，对齐 `atengk/oss-template` 标准，采用 `docker/metadata-action@v5` 的声明式规则：
   - 显式声明 `flavor: latest=auto`；
   - 标签生成矩阵配置：
     - `type=semver,pattern={{version}}`（例如生成 `1.3.1`）
     - `type=semver,pattern={{major}}.{{minor}}`（例如生成 `1.3`）
     - `type=semver,pattern={{major}}`（例如生成 `1`）
   - **预发布防污染机制**：当发布带连字符的预发布版本（如 `v1.4.0-rc.1`）时，`latest=auto` 机制将自动拦截生成 `latest` 标签，彻底避免预发布测试镜像污染生产默认镜像。

3. **版本不可变性 (Tag Immutability)**：
   遵循软件工程发版规范，已发布的 `v1.3.0` 标签保持不可变，本次流水线优化与容器镜像发布统一作为 `v1.3.1` 补丁版本发布。
