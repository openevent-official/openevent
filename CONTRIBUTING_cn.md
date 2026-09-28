# 参与贡献

[English version](CONTRIBUTING.md)

## 开发与验证

按 [README](README_cn.md#构建) 安装依赖并初始化子模块，然后运行：

```bash
make test
make test-runtime
make check-docs
```

`make test-runtime` 需要当前 Python 环境已安装 `openevent-sdk>=0.10.0`、`pytest` 和 `packaging`。
使用 `PYTHON` 选择 Python 环境。测试数据与日志保存在 `build/`。

## 贡献准则

- 行为变更应增加或更新测试，并同步更新公开文档及其中英文翻译。
- gRPC 协议和 API 契约在 [SDK 仓库](https://github.com/openevent-official/openevent-sdk) 中维护；
  涉及 SDK 的修改按其贡献与验证说明进行。
- 公开文档聚焦使用方式、配置和 API 行为，链接应支持 GitHub 直接阅读。
- 不提交生成文件、运行数据、凭据或日志。

## Pull Request

保持变更小而可验证。PR 说明应包含改动目的、验证命令和结果，以及兼容性影响（如有）。
