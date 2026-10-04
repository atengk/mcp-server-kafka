# 任务追踪器：GitHub Issues (Issue tracker: GitHub)

本项目的缺陷报告、特性需求与规格说明（Spec）统一存储为 GitHub Issues。所有工程技能均基于 `gh` CLI 命令行工具开展自动化操作。

## 常用操作规范 (Conventions)

- **创建 Issue**：`gh issue create --title "..." --body "..."`。多行内容建议使用标准 Heredoc 传入。
- **查看 Issue 详情**：`gh issue view <编号> --comments`，配合 `jq` 解析过滤评论并读取所挂载的 Labels。
- **检索 Issue 列表**：
  ```bash
  gh issue list --state open --json number,title,body,labels,comments \
    --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'
  ```
  结合实际需要追加 `--label` 和 `--state` 过滤参数。
- **追加评论**：`gh issue comment <编号> --body "..."`
- **挂载与移除标签**：`gh issue edit <编号> --add-label "..."` / `--remove-label "..."`
- **关闭 Issue**：`gh issue close <编号> --comment "..."`

仓库地址自动从本地 `git remote -v` 解析推断，在仓库克隆目录下运行 `gh` 命令即可自动绑定目标仓库。

## 将 PR 纳入分流处理范围 (Pull requests as a triage surface)

**将 PR 作为需求流转源面：否 (PRs as a request surface: no)**。  
*(若后续需要将外部贡献者的 PR 同步纳入 `/triage` 分流队列，可将此项修改为 `yes`)*

当开启为 `yes` 时，PR 将复用与 Issue 相同的标签体系与状态流转机制，对应的 `gh pr` 指令如下：
- **查看 PR**：`gh pr view <编号> --comments`，并通过 `gh pr diff <编号>` 获取代码变动 Diff。
- **获取待分流的外部 PR**：
  ```bash
  gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments
  ```
  随后仅筛选 `authorAssociation` 为 `CONTRIBUTOR`、`FIRST_TIME_CONTRIBUTOR` 或 `NONE` 的外部提交（排除 `OWNER` / `MEMBER` / `COLLABORATOR`）。
- **评论 / 标签变更 / 关闭**：对应使用 `gh pr comment`、`gh pr edit --add-label`/`--remove-label`、`gh pr close`。

> 💡 *注意：GitHub 的全局序号池在 Issue 与 PR 之间共享，单个 `#42` 可能代表 PR 也可能代表 Issue。建议优先使用 `gh pr view 42` 探测，若不存在则回退至 `gh issue view 42`。*

## 当技能提示“发布至任务追踪器 (publish to the issue tracker)”

自动调用 `gh issue create` 创建一个全新的 GitHub Issue。

## 当技能提示“提取相关工单 (fetch the relevant ticket)”

自动执行 `gh issue view <编号> --comments` 获取完整工单上下文与讨论。

## 路线指引编排规范 (Wayfinding operations)

供 `/wayfinder` 技能联动使用。整个路线**全景图 (Map)** 对应一个独立的父级 Issue，而**子任务 (Child tickets)** 则以关联 Issue 形式挂载。

- **路线全景图 (Map)**：单个挂载 `wayfinder:map` 标签的父级 Issue，主体包含：阶段梳理 (Notes)、既定决策清单 (Decisions-so-far)、未知迷雾区 (Fog)。创建命令：`gh issue create --label wayfinder:map`。
- **子任务工单 (Child ticket)**：通过 GitHub sub-issue 机制关联至父级 Map 工单（通过 `gh api` 对应 sub-issues 端点）。若当前仓库尚未开启 sub-issues，则在 Map 主体任务列表中记录子项，并在子工单主体顶部注明 `Part of #<Map编号>`。子任务标签统一遵循 `wayfinder:<type>`（如 `wayfinder:research`、`wayfinder:prototype`、`wayfinder:grilling`、`wayfinder:task`）。一旦被认领，该工单将自动指派给推进开发者。
- **依赖阻断关系 (Blocking)**：优先采用 GitHub 原生 **Issue Dependencies** 依赖能力。调用接口新增阻断边：
  ```bash
  gh api --method POST repos/<所有者>/<仓库名>/issues/<子工单编号>/dependencies/blocked_by \
    -F issue_id=<前置阻塞工单数据库ID>
  ```
  *(注：`<前置阻塞工单数据库ID>` 为该工单的全局数值数据库 ID，可通过 `gh api repos/<所有者>/<仓库名>/issues/<n> --jq .id` 获取，并非 `#number` 或 `node_id`)*。若原生依赖不可用，可在子工单顶部标注文本行：`Blocked by: #<n>, #<n>`。当所有前置阻塞工单全部关闭后，子工单即告解除阻断。
- **前沿就绪任务查询 (Frontier query)**：列出 Map 下属全部未完结子任务，排除任何存在未关闭阻塞项的任务（即 `issue_dependencies_summary.blocked_by > 0` 或在 `Blocked by` 标注中有未关闭 Issue）以及已有指派人的任务，按 Map 编排顺序首位优先出列。
- **任务认领 (Claim)**：`gh issue edit <编号> --add-assignee @me`（认领为会话开始的第一笔写操作）。
- **任务完成结算 (Resolve)**：调用 `gh issue comment <编号> --body "<产出结论>"`，随后 `gh issue close <编号>`，并将对应决策摘要指针（Gist + 链接）回填追加至 Map 的既定决策清单中。
