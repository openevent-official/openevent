# 参与贡献

[English version](CONTRIBUTING.md)

感谢你关注 OpenEvent。

## 开发环境

先安装 [README](README_cn.md) 中列出的依赖，然后运行：

```bash
make test
```

## 贡献准则

- 公开行为应记录在 `openevent-sdk/docs/API_cn.md`，并同步更新对应的英文翻译。
- 修改 gRPC 契约时，按 service 边界和共享 package 类型同步更新 `openevent-sdk/proto/openevent.proto` 或
  `openevent-sdk/proto/admin.proto`。
- Python SDK 的 Protobuf 文件由 `openevent-sdk` 的 `make build` 和 `make test` 生成，不进 Git。
  这两个 target 必须生成这些文件，不能假定工作区已存在生成产物。
- 修改 proto 或 SDK 相关文件后，还必须按照 `openevent-sdk/README_cn.md` 验证 SDK 子模块。
- 优先提交小而可验证的变更；行为变更应增加或更新测试。
- 行为、配置或公开契约变更时，同步更新文档。
- 公开文档只描述使用方式、配置和 API 行为，避免记录内部设计和实现细节。
- 不提交构建产物、RocksDB 数据、临时 token、日志或私人笔记。
- Markdown 链接应适用于 GitHub 直接阅读。

## Pull Request

提交 PR 前请确认构建和测试已通过、文档已更新。PR 应包含：

- 清晰的变更说明。
- 构建和测试验证使用的命令。
- 兼容性影响说明（如有）。
- 公开行为变更对应的文档更新。
