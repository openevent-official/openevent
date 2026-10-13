# 参与贡献

[English version](CONTRIBUTING.md)

## 开发与验证

按 [README](README_cn.md#构建) 安装依赖并初始化子模块，然后运行：

```bash
make test
make test-runtime
make check-docs
```

`make test-runtime` 先构建当前服务端，并通过 SDK 的 `make test-env` 从本地源码构建、安装本次 SDK wheel，
再使用该测试环境运行；SDK 测试环境和依赖规则见 [SDK README](openevent-sdk/README_cn.md#构建和测试)。
使用 `PYTHON` 选择创建测试环境的 Python。服务端测试数据与日志保存在 `build/`。

## 贡献准则

- 行为变更应增加或更新测试，并同步更新公开文档及其中英文翻译。
- gRPC 协议和 API 契约在 [SDK 仓库](https://github.com/openevent-official/openevent-sdk) 中维护；
  涉及 SDK 的修改按其贡献与验证说明进行。
- 公开文档聚焦使用方式、配置和 API 行为，链接应支持 GitHub 直接阅读。
- 不提交生成文件、运行数据、凭据或日志。

## Pull Request

保持变更小而可验证。PR 说明应包含改动目的、验证命令和结果。
