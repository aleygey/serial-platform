# Issue 驱动的评估、修复与发布流水线

本文描述一条适合本仓库的 GitHub Issue -> 本地 Codex -> Pull Request ->
GitHub Actions/Jenkins -> GitHub Release 流程。它是实施设计，不会因为合入本文而自动启用。

## 目标与非目标

目标：

- 由 GitHub Issue 记录需求、缺陷、验收条件和运行状态；
- 本地 Agent 在隔离 worktree 中评估和修复，不污染开发者正在使用的工作区；
- 所有代码通过 Pull Request 和确定性检查进入 `main`；
- Jenkins 只从确定 commit 构建，只有受信任的版本 tag 才能发布 Release；
- 每一步都可以从 Issue、PR、commit、Jenkins build 和 Release 反向追溯。

不建议把“任何人新建 Issue”直接等同于“在本地执行任意代码并发布”。Issue
标题、正文、评论和附件都是不可信输入，必须先经过身份检查和明确的 label 审批。

## 当前仓库已经具备的能力

- `.github/workflows/ci.yml` 在 PR 和 `main` push 上执行 Rust format、workspace
  tests、Clippy、Rust 1.88 MSRV 以及 Electron build。
- `.github/workflows/release.yml` 对 `codex/**` 分支执行 Linux、Windows、macOS
  的非发布原生验证。
- `Jenkinsfile` 将第一次 checkout 的完整 commit 固定成带 SHA-256 的 Git bundle，
  后续各节点只使用这份 bundle。
- `Jenkinsfile` 仅在当前 commit 带有与 workspace 版本一致的 annotated
  `vX.Y.Z` tag 时切换到 release 构建。
- `ci/publish-github-release.sh` 校验本地 tag、远端 annotated tag、tag commit、
  四个平台资产、精确资产集合和 SHA-256，并以可重试方式发布 Release。
- Jenkins 已使用 `disableConcurrentBuilds()`，发布脚本对相同资产的重试是幂等的。

因此，新流水线应复用现有 Jenkins 发布链，而不是增加第二个 Release 发布者。

## 2026-08-24 外部状态审计

GitHub 公开 API 显示：

- 仓库是 public，Issue 功能已启用，当前没有 open Issue；
- `main` 和所有公开分支均显示 `protected=false`，仓库 ruleset 列表为空；
- GitHub Environments 列表为空，因此没有可见的 release required-reviewer 闸门；
- labels 仍只有 GitHub 默认集合，没有下文建议的 Agent 状态 labels；
- 当前 `main` 的 v0.8.3 CI run 成功，四个准确的 checks 是
  `Minimum Rust 1.88`、`Validate (windows-latest)`、
  `Validate (ubuntu-latest)` 和 `Electron desktop`；
- v0.8.3 Release 已发布并带五个资产；
- Actions API 仍将历史的 `cloud-release-v0.8.2.yml` 和
  `promote-v0.8.2.yml` 登记为 active workflow，且未保护的
  `release/v0.8.2` 分支仍存在，尽管这些 workflow 文件已不在当前 `main`；
- v0.8.2 的历史 run 确实曾用临时 GitHub Actions workflow 和
  `contents: write` 发布，而当前仓库文档声明 Jenkins 是唯一发布者。

因此，“Jenkins-only”目前是代码约定，不是 GitHub 权限层面的强制策略。启用 Issue
worker 前应先保护 `main`/`v*`、建立 release environment、显式 disable 已退役的
one-shot workflows、删除不再需要的 release branch，并确认 Actions/Jenkins 的发布
凭据不能被 Agent 身份取得。

## 推荐流程

```text
Issue opened/edited
    |
    | owner adds agent:triage-approved
    v
read-only evaluation in isolated worktree
    |
    +--> agent:needs-info + Issue comment
    |
    +--> structured plan + risk/test report
             |
             | owner adds agent:fix-approved
             v
      codex/issue-<number>-<slug>
             |
             v
      local deterministic checks
             |
             v
        draft Pull Request
             |
             +--> GitHub Actions CI
             +--> release validation on codex/**
             +--> Jenkins debug build through push webhook
             |
             | required checks + human approval
             v
            main
             |
             | separate release PR / release:approved gate
             v
      version bump + annotated vX.Y.Z tag
             |
             | GitHub push webhook
             v
       Jenkins tag build
             |
             v
    verified GitHub Release
```

代码合并和产品发布应是两个状态机。普通 Issue 修复合并后不应自动生成一个新版本；
发布可以聚合多项修复，并以独立 release PR 和版本 tag 作为唯一授权信号。

## Issue 状态与审批

建议创建以下 labels：

| Label | 含义 | 谁可以设置 |
| --- | --- | --- |
| `agent:candidate` | 可被 Agent 评估，但尚未授权执行 | triage bot 或维护者 |
| `agent:triage-approved` | 授权只读评估 | 维护者 allowlist |
| `agent:evaluating` | 本地 worker 已领取评估 | worker |
| `agent:needs-info` | 验收条件或复现信息不足 | worker |
| `agent:fix-approved` | 授权创建 worktree、修改代码和开 PR | 维护者 allowlist |
| `agent:implementing` | 本地 worker 已领取修复 | worker |
| `agent:pr-open` | 已创建 PR | worker |
| `agent:failed` | 自动步骤失败，需要人工处理 | worker |
| `agent:done` | PR 已合并或 Issue 已完成 | bot/维护者 |
| `release:approved` | 授权创建 release PR 或 tag | release maintainer |

只检查 label 是否存在不够。触发器还必须确认添加审批 label 的事件 actor 位于固定的
维护者 allowlist，或通过 GitHub API 确认其 repository permission 至少为 `maintain`。
删除后重新添加 label 应生成新的审计记录。

审批还必须绑定到维护者实际审阅过的 Issue 内容版本。添加
`agent:triage-approved` 或 `agent:fix-approved` 时，控制面记录 Issue `updated_at`、
标题、正文、附件 URL 及明确纳入需求的评论 ID/正文，并对规范化结果计算 SHA-256。
任何后续标题/正文/附件编辑、新评论或已纳入评论的编辑，都会使对应审批记录失效；
bot 应立即移除审批 label（或添加明确的 `agent:approval-stale` 状态），停止尚未开始的
任务，并要求 allowlist 内维护者对新摘要重新批准。轮询与 webhook 两条入口必须使用
同一套快照算法，不能让旧 label 授权修改后的提示内容。

建议单 worker 串行领取任务。这样可以避免两个本地 worker 同时看到
`agent:fix-approved` 后重复开分支。未来需要并发时，应使用带唯一约束的外部任务表，
不能把 GitHub label 当作原子锁。

## 本地触发方式

### 第一阶段：出站轮询，推荐

由 Codex 桌面 Scheduled task 或受约束的本地 worker 每 2-5 分钟查询一次：

```text
is:issue is:open label:agent:triage-approved -label:agent:evaluating
is:issue is:open label:agent:fix-approved -label:agent:implementing
```

查询命中只是候选条件。worker 领取任务前必须从 GitHub API 读取最新 Issue 与 timeline，
确认审批 actor、审批事件和已保存内容哈希仍匹配；不匹配时只标记审批过期，不执行
评估或修复。

出站轮询不需要把开发机或 Jenkins 暴露到公网。Codex 桌面 scheduled task 可在本地
项目或隔离 worktree 中运行，但电脑必须保持开机且应用运行。若要求秒级事件触发，
再升级到下一阶段。

### 第二阶段：签名 webhook + 本地队列

GitHub App 接收 `issues`、`issue_comment` 和 `pull_request` webhook，入口只做：

1. 校验 `X-Hub-Signature-256`；
2. 校验 delivery ID，拒绝重放；
3. 校验 repo、event type、actor、审批 label 和已批准内容快照；
4. 将规范化的 issue number、repo、delivery ID 和审批记录 ID 写入本地持久队列；
5. 立即返回 2xx，不在 HTTP handler 中执行 Agent。

worker 再从 GitHub API 重新读取 Issue 当前状态，并在启动 Agent 前重新计算内容哈希；
它必须与队列引用的审批记录完全一致，不能信任排队时的 webhook 正文。编辑或评论类
webhook 还应主动使旧审批失效，避免任务在下一轮轮询前抢先启动。
公网入口应通过反向代理或受控 tunnel，仅暴露 webhook endpoint，不暴露 Jenkins UI、
Codex App Server 或本地 shell。

## Agent 执行边界

评估和修复必须分成两个独立运行：

1. 评估阶段使用只读仓库，输出固定 JSON schema：范围、复现、根因假设、修改文件、
   测试计划、风险级别、是否涉及持久化/协议/发布配置、是否需要人工信息。
2. 修复阶段只在内容哈希仍匹配的 `agent:fix-approved` 后创建
   `codex/issue-<number>-<slug>` worktree；执行中若收到 Issue 编辑/评论事件，应在下一个
   安全中断点停止，保留审计日志并等待重新批准。
3. Issue 内容只作为带明确边界的“不可信需求数据”传入，不能拼接到 shell 命令、
   路径、branch name、测试名称或环境变量中。
4. worker 使用固定的命令 allowlist。默认至少运行：
   `cargo fmt --all -- --check`、相关 crate tests、
   `cargo test --workspace --locked`、
   `cargo clippy --workspace --all-targets --locked -- -D warnings` 和
   `npm run build`（desktop 变更时）。
5. Agent 进程不持有 Release token、Jenkins 管理凭据或 GitHub admin 权限。
6. worker 只允许 push `codex/issue-*` 分支并创建 draft PR，不允许直接 push `main`、
   创建/移动 tag、修改 branch protection 或发布 Release。
7. 日志和 Issue 评论只发布测试摘要、commit SHA 和可公开错误；环境变量、完整配置、
   token、私钥和本地绝对路径必须脱敏。

## GitHub 身份与最小权限

优先使用单仓库 GitHub App installation token，不使用个人 SSH key 或 classic PAT。

本地 issue worker 建议权限：

- Metadata: read；
- Issues: read/write；
- Pull requests: read/write；
- Contents: read/write，但 GitHub App 不加入 branch-protection bypass list；
- Checks/Actions: read。

明确禁止：Administration、Members、Secrets、Actions write、Workflows write 和
Releases write。即使 worker 的 Contents 权限可以创建分支，`main` 仍必须由 branch
protection 拒绝直接 push。

Jenkins publisher 使用另一个身份，只授予目标仓库的 Contents/Releases write。
当前 Jenkins credential ID 是 `github-release-token`；实施时应确认它是 fine-grained
PAT 或短期 GitHub App token，并安排轮换，不要与 issue worker 共用。

若改用 `openai/codex-action`，`OPENAI_API_KEY` 只存 GitHub Secret；必须限制触发用户、
使用最窄 sandbox、保留默认降权策略，并清理 Issue/PR 的 prompt injection 内容。
不要在 `pull_request_target` 中 checkout 或执行来自 fork 的代码。

## Pull Request 合并闸门

建议为 `main` 启用 ruleset/branch protection：

- 禁止直接 push 和 force push；
- 要求 PR；
- 至少 1 名非 Agent 维护者批准；
- 要求新提交后旧批准失效；
- 要求所有 review conversation resolved；
- 要求 branch up to date；
- 要求现有 GitHub Actions CI 全部成功；
- 将 Jenkins debug build 以 commit status/check 的形式回写，并设为 required；
- 保护 `v*` tag，只有 release bot/maintainer 可创建，任何人都不可移动或覆盖；
- Agent GitHub App、Jenkins 身份和 release bot 都不绕过 `main` 保护。

可以在稳定运行后，为明确标记 `agent:auto-merge-approved` 的低风险 PR 开启自动合并；
没有该第二个人工 label 时不自动 merge。

## Jenkins webhook 与 tag 构建

仓库内 `Jenkinsfile` 没有 `triggers` block，因此是否已接收 webhook、构建哪些 refs、
如何回写 GitHub checks 取决于 Jenkins job 外部配置。

推荐把 job 配成 GitHub Branch Source multibranch/organization folder：

- 启用 GitHub webhook；
- 发现 `main`、`codex/**`、PR 和 annotated `v*` tags；
- branch/PR push 运行 debug 构建；
- tag job 的 `checkout scm` 必须 checkout webhook 对应的 tag commit；
- Jenkins 必须把 build result 回写到该 commit；
- webhook delivery 使用 secret 并记录 delivery ID；
- 不接受 Issue 正文作为 Jenkins 参数。

不要用一个始终 checkout “当前 main”的单分支 job 处理 tag webhook。tag 创建与 main
继续前进之间存在竞态，job 可能构建到后续 commit，现有 Jenkinsfile 会因此退化为
debug 且不发布。tag discovery job 能让 `checkout scm` 固定到 tag 指向的 commit。

## 版本与发布

当前版本需要同步修改：

- workspace `Cargo.toml`；
- `Cargo.lock` 中五个本地 crate；
- `crates/serial-desktop/package.json`；
- `crates/serial-desktop/package-lock.json`。

仓库目前没有统一 bump 脚本、release PR workflow 或 changelog 生成器。最小实现应增加
一个可测试的 `ci/set-version.sh X.Y.Z`，只接受严格 SemVer，更新上述文件后运行
`cargo metadata --locked` 和 desktop version 一致性检查。发布流程建议：

1. release bot 根据 `release:approved` 创建 release PR；
2. 维护者确认版本、release notes 和所包含的 Issue；
3. release PR 通过全部 required checks 后合并；
4. 受保护环境中的 release job 在该 merge commit 创建 annotated（最好签名的）tag；
5. tag push webhook 触发 Jenkins tag job；
6. Jenkins 沿用现有四平台打包、摘要验证和 Release 发布脚本；
7. 成功后 bot 关闭对应 Issue，并写入 Release URL、tag、commit 和 Jenkins build URL。

发布失败时不要移动或覆盖 tag。修复问题后使用新 patch 版本；如果只是 Jenkins
临时失败，可以对同一 tag 重新构建，现有 publisher 会保留字节完全一致的资产并拒绝
不一致资产。

## 当前缺口

启用自动化前需要补齐：

- 根目录没有 `AGENTS.md`，尚未固化项目测试、兼容性和 review 规则；
- 没有 Issue form、CODEOWNERS、contribution/security policy；
- 没有 issue-agent workflow、本地 worker、任务状态存储和结构化评估 schema；
- 没有统一版本 bump 脚本、release PR 和 tag 创建 workflow；
- GitHub branch/ruleset、tag protection、environment approval 和 webhook 状态不在仓库内；
- Jenkins webhook、multibranch tag discovery、GitHub check 回写和 credential scope
  需要在 Jenkins 管理界面确认；
- Actions 依赖目前使用 `@v4`/`@stable` 等浮动引用。高信任流水线应固定到审计过的
  full commit SHA，并由 Dependabot/Renovate 单独升级；
- GitHub Release 仓库名在 `ci/publish-github-release.sh` 中硬编码，仓库迁移或 fork
  时需要显式处理；
- macOS App 尚未 Developer ID 签名或 notarize；自动发布不会自动解决安装信任问题。

## 分阶段上线

1. **观察模式**：只读轮询和结构化评估，只在 Issue 评论计划，不改文件。
2. **PR 模式**：审批后允许 worktree、分支和 draft PR，不自动 merge。
3. **受控合并**：配置 required checks、CODEOWNERS 和 Jenkins status 后，允许维护者
   label 触发 auto-merge。
4. **受控发布**：上线 release PR、受保护 tag 和 Jenkins tag job；tag 仍保留人工闸门。
5. **按策略自动发布**：只有前四阶段稳定并有审计记录后，才考虑让低风险 patch
   release 自动创建 tag；重大版本、协议/schema 变化和安全修复继续人工批准。

每一阶段至少演练成功、测试失败、Agent 中断、重复 delivery、批准后编辑正文、批准后
追加/修改评论、GitHub API 失败、Jenkins 失败和 Release 重试，再进入下一阶段。测试要
确认内容变化会使审批失效且不会创建 worktree、运行命令或继续已有 Agent。

## 外部配置与 Secrets 清单

GitHub：

- issue labels、Issue form、CODEOWNERS；
- `main` ruleset 与 `v*` tag ruleset；
- required GitHub Actions/Jenkins checks；
- release environment + required reviewer；
- GitHub App 安装及 worker/release 身份分离；
- webhook secret（仅在第二阶段 webhook 方案中需要）；
- `OPENAI_API_KEY`（仅在 GitHub Action 运行 Codex 时需要）。

本地 worker：

- GitHub App ID、installation ID 和 private key，存系统 keychain/secret store；
- Codex 登录或 API 凭据，存 Agent 自身凭据存储；
- 独立 worktree 根目录、单实例锁、持久任务队列、审批内容快照和审计日志；
- 固定 repo allowlist、actor allowlist、命令 allowlist 和并发上限。

Jenkins：

- GitHub webhook integration 和 tag discovery；
- `github-release-token` 的最小权限与轮换策略；
- Linux/Windows cross-build 节点及 `rust-macos && arm64` 节点；
- GitHub commit status/check 回写凭据；
- webhook secret、构建保留策略和备份。

## 参考

- OpenAI Codex Scheduled tasks：<https://learn.chatgpt.com/docs/automations>
- OpenAI Codex GitHub Action：<https://learn.chatgpt.com/docs/github-action>
- OpenAI Codex GitHub code review：<https://learn.chatgpt.com/docs/third-party/github>
