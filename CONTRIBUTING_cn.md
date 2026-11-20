# 参与贡献

[English version](CONTRIBUTING.md)

## 开发与验证

按 [README](README_cn.md#构建) 安装依赖并初始化子模块。运行时测试和 SDK 端到端测试会把本次构建的 SDK
装入 `PYTHON` 选定的开发环境。可以使用已有环境，或在服务端目录执行示例的前两行，创建并选择一个环境。

```bash
python3 -B -m venv build/venv
export PYTHON="$PWD/build/venv/bin/python"
make test test-runtime &&
    OPENEVENT_SERVER_BIN="$PWD/build/openevent_server" make -C openevent-sdk e2e
```

`make build` 配置 CMake，从 SDK 子模块的协议文件生成 C++ 代码并编译服务端。`make install` 安装构建出的可执行文件。
即使上次构建关闭了测试，`make test` 也会在当前构建目录启用测试，运行 C++ 核心测试并检查服务端是否全静态链接。
CTest 未发现任何测试时会报错。

`make test-runtime` 先构建当前服务端，再调用 SDK 的 `make test-env` 安装本次 wheel 和测试依赖，
然后用同一个 Python 运行服务端测试。使用 `make test test-runtime` 时，两套测试共用一次服务端构建。
服务端测试数据与日志保存在 `build/`。
环境选择和安装规则见 [SDK README](openevent-sdk/README_cn.md#构建和测试)。
SDK 的 `make e2e` 使用上面构建的服务端验证客户端能否通过 RPC 完成各项操作，包括批量查询对象元数据。

## 贡献准则

- 行为变更应增加或更新测试，并同步更新公开文档及其中英文翻译。
- gRPC 协议和 API 契约在 [SDK 仓库](https://github.com/openevent-official/openevent-sdk) 中维护；
  涉及 SDK 的修改按其贡献与验证说明进行。
- 公开文档聚焦使用方式、配置和 API 行为，读者应当能在 GitHub 上直接打开文档中的链接。
- 不提交生成文件、运行数据、凭据或日志。

## Pull Request

保持变更小而可验证。PR 说明应包含改动目的、验证命令和结果。
