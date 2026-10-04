# 贡献指南 (Contributing Guide)

感谢你关注并愿意为本项目贡献力量！为了保持高效协作与高质量的代码维护，请在提交代码前阅读以下规范。

---

## 1. 协作与分支模型

本项目遵循标准的 **GitHub Flow** 工作流：

1. **Fork 本仓库** 到你个人的 GitHub 账号；
2. **基于 `main` 分支拉取新的特性分支**：
   ```bash
   git checkout -b feat/your-feature-name
   # 或者缺陷修复分支
   git checkout -b fix/issue-description
   ```
3. 在本地完成修改，确保自测通过并补充相应测试用例；
4. 提交更改并推送到你的远程分支：
   ```bash
   git push origin feat/your-feature-name
   ```
5. 在 GitHub 上向本仓库的 `main` 分支发起 **Pull Request**。

---

## 2. Commit 提交信息规范

本项目遵循 [Conventional Commits](https://www.conventionalcommits.org/zh-hans/) 规范，统一采用以下格式：

```text
<type>(<scope>): <subject>
```

### 常用类型说明

| 类型 | 说明 | 示例 |
| :--- | :--- | :--- |
| `feat` | 新增功能或特性 | `feat(producer): 支持发送分区消息` |
| `fix` | 缺陷与 Bug 修复 | `fix(consumer): 修复消息反序列化异常` |
| `docs` | 仅文档更新或修改 | `docs: 完善快速开始与环境配置文档` |
| `style` | 代码格式调整（空格、分号等，不影响逻辑） | `style: 优化代码排版与换行` |
| `refactor` | 代码重构（既非新增特性也非修复缺陷） | `refactor(core): 优化客户端连接管理` |
| `perf` | 性能优化 | `perf(pool): 优化连接池复用提升吞吐` |
| `test` | 增加或重构单元测试与集成测试 | `test: 补充主题创建边界测试用例` |
| `build` | 构建系统、外部依赖或脚手架调整 | `build: 升级 aiokafka 依赖版本` |
| `ci` | CI/CD 流水线与 GitHub Actions 脚本修改 | `ci: 优化 release 自动化发版流程` |
| `chore` | 其他琐碎杂项（不改动源码与测试） | `chore: 更新 .gitignore 忽略规则` |
| `revert` | 恢复或回滚此前的某次历史提交 | `revert: feat(producer): 回退消息发送逻辑` |

---

## 3. Pull Request 流程

- **PR 标题规范**：PR 标题必须同样遵循 [Conventional Commits](#2-commit-提交信息规范) 格式（如 `feat: 新增能力` 或 `fix: 修复缺陷`），CI 会对其进行自动化合规校验；
- **模版填写**：发起 PR 时，请按模版完整填写变更背景、解决的问题以及关联的 Issue（如 `close #12`）；
- **CI 绿灯**：确保 CI 流水线测试全部处于通过状态；
- **审查与合并**：代码审查（Code Review）提出修改意见后，在原分支继续提交即可自动同步至 PR；合并后特性分支将被删除。

---

## 4. 版本发版机制与发布说明

本项目通过 GitHub Actions 实现了自动化发版体系。正式发版标准流程如下：

### 1. 日常提交与日志归纳
平时向 `main` 分支提交代码或合并 PR 时，规范的提交记录（Conventional Commits）会被 `git-cliff` 自动追踪，并在发版时聚合生成更新日志。

### 2. 同步项目版本号
发版前在 `pyproject.toml` 与 `src/mcp_server_kafka/__init__.py` 中同步递增 `version` 字段：
```bash
# 递增后在本地提交
git commit -am "chore(release): v1.1.0"
```

### 3. 打标签并推送到远端（触发发版）
当本地代码与版本准备就绪后，推送标签至 GitHub 即可触发发版流水线：

```bash
# 步骤 A：确保本地最新代码已推送到 main 分支
git push origin main

# 步骤 B：打版本标签并推送到 GitHub (如 v1.1.0, v1.2.0 等)
git tag v1.1.0
git push origin v1.1.0
```

### 4. 自动化流水线运行
标签推送后，GitHub Actions 将会自动执行 [`.github/workflows/release.yml`](./.github/workflows/release.yml)：
- 自动提取自上一版本以来的全部提交与 PR，由 `git-cliff` 格式化生成更新日志；
- 编译打包 Wheel 与源码包，计算 `checksums.txt` SHA-256 校验和并创建 GitHub Release 挂载附件；
- 自动通过 `uv publish` 发布安装包至 [PyPI 官方仓库](https://pypi.org/project/atengk-mcp-server-kafka/)；
- 自动构建并推送纯净无杂质的多架构 (`linux/amd64`, `linux/arm64`) Docker 镜像至 GitHub Container Registry (ghcr.io)。
