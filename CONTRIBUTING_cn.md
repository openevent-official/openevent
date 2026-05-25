# 参与贡献

[English version](CONTRIBUTING.md)

感谢你关注 OpenEvent。

## 开发环境

先安装 [README](README_cn.md) 中列出的依赖，然后运行：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

## 贡献准则

- 公开行为应记录在 `openevent-sdk/docs/API.md`。
- 修改 gRPC 契约时，同步更新 `openevent-sdk/proto/openevent.proto`。
- Python SDK 的 Protobuf 文件由 `openevent-sdk` 的 `make build` 和 `make test` 生成。
- 行为变更应增加或更新测试。
- 公开文档只描述使用方式、配置和 API 行为，避免记录内部设计和实现细节。
- Markdown 链接应适用于 GitHub 直接阅读。

## Pull Request

提交 PR 前请确认：

- 项目可以成功构建。
- 测试已通过。
- 相关文档已更新。
- 没有提交构建产物、本地数据或私人笔记。
