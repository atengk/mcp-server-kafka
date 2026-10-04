# 项目智能体协作规范

本项目面向工程化智能体（AI 编码助手）制定了统一的工作流与规范。在开展任何架构探索、工单处理或代码修改前，请遵循以下约定：

## 智能体技能工程体系

### 任务与工单追踪器

本项目的需求规格与缺陷任务统一在 GitHub Issues 中追踪管理（使用 `gh` 命令行工具操作）。详情参见 [`docs/agents/issue-tracker.md`](./docs/agents/issue-tracker.md)。

### 分流标签词汇体系

采用五大标准分流角色标签（`needs-triage`、`needs-info`、`ready-for-agent`、`ready-for-human`、`wontfix`）。详情参见 [`docs/agents/triage-labels.md`](./docs/agents/triage-labels.md)。

### 领域文档架构布局

采用单上下文架构（根目录维护 [`CONTEXT.md`](./CONTEXT.md)，架构决策记录统一存放于 [`docs/adr/`](./docs/adr/) 目录）。详情参见 [`docs/agents/domain.md`](./docs/agents/domain.md)。
