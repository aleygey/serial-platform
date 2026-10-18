# GitHub Actions 构建与发布

自 v0.8.9 起，GitHub Actions 是 Serial Platform 的唯一正式发布入口。无需启动本地 Jenkins、借用个人电脑资源或配置新的长期访问密钥。

## 正常发布

1. 在修复分支更新工作区、Cargo.lock、桌面 package.json/package-lock.json 的一致版本，并准备 `docs/releases/vX.Y.Z.md`。
2. Pull Request 的 CI 与原生验证通过后合入 `main`。涉及发布工作流、打包脚本、版本清单或更新说明的 PR 还会只读运行同一四平台打包与完整资产验证，绝不创建 tag 或 Release。
3. `Publish release`（`.github/workflows/publish-release.yml`）由 main push 自动启动，固定这次推送的完整 commit SHA。版本已正式发布时直接跳过，不重复打包或覆盖旧版本。
4. 未发布版本由标准 GitHub 托管 runner 构建 Linux x86_64、Windows x86_64、macOS arm64、macOS x86_64。复用原有打包脚本的版本、架构、兼容性、MCP 工具数、应用 sidecar 和包内清单检查。
5. 所有测试、打包与验收任务成功后，发布任务创建或验证 annotated `vX.Y.Z` tag，使其精确指向该源码 commit；汇总 `SHA256SUMS`，先上传草稿并验证四个包及其校验和，最后才公开正式 Release。

也可在 GitHub Actions 页面选中 `Publish release`，从 `main` 手动运行，用于未完成版本的重试。手动运行不是绕过检查的快速发布入口。

## 资产与权限

- Release 包含四个平台安装压缩包和一个 `SHA256SUMS`；压缩包继续包含四个 Rust 程序、Electron App、`BUILD-INFO.json` 和清单。
- 构建任务默认只有读取代码的权限。只有依赖全部验收任务成功的发布任务使用短期 `GITHUB_TOKEN` 的 `contents: write`；不需要 Personal Access Token、SSH key 或自托管 runner。
- 不接受外部 PR 触发写权限发布，不使用 `pull_request_target`。所有阶段使用同一固定源码 SHA。
- 中间 Actions artifacts 保留 3 天，正式下载从 GitHub Release 获取。中间产物不是正式发布结果。
- 使用标准托管 runner，不启用 larger runners，也不修改仓库计费设置。公开仓库标准 runner 的政策以 GitHub 当前说明为准；不要把公开仓库转为私有后仍假定同样的免费额度。

## 失败和重试

- 某个平台失败：Release 不公开。查看最先失败的 job/step，在分支修复后再次合入，或对临时下载故障重跑失败任务。
- 已有 tag 指向其他提交、不是 annotated tag，或已有正式 Release 资产与当前文件不一致：停止，不强推 tag、不覆盖资产。
- 草稿上传中断：现有发布脚本按 Release/asset ID 校验已上传内容，相同资产可保留并补齐；不同内容拒绝覆盖。
- 首次运行若提示 Actions 被仓库或组织策略禁用，由仓库管理员在 GitHub 设置中处理；本流水线不会自动更改这些安全设置。
- 发布成功必须同时核对 Release 可见、tag commit、四个平台资产和 `SHA256SUMS`，不能只凭某个 job 显示绿色。

## Jenkins 迁移边界

根目录 `Jenkinsfile` 已替换为不申请 agent、不读取凭证、不构建也不发布的提示流程，避免旧 webhook 造成第二个发布者。这不会停止或删除现有 Jenkins 服务、节点、任务、凭证或历史构建。

旧 `docs/ISSUE_DRIVEN_DELIVERY.md` 中的 Jenkins 流程保留作历史设计参考，不再代表当前发布政策。无需继续向 Jenkins 推送版本 tag 来启动新发行版。

## 验证边界

CI、模拟后端、原生二进制 smoke 和 Electron 交互检查不能替代真实串口设备测试。macOS App 仍未配置 Developer ID 签名或 notarization，本次迁移不创建相关凭证。
