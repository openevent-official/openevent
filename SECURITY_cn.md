# 安全策略

[English version](SECURITY.md)

## 支持版本

在 OpenEvent 发布稳定版本前，安全修复以默认分支为准。

## 报告漏洞

如果公开仓库已启用 GitHub private vulnerability reporting，请优先通过该渠道报告。
如果尚未启用，请创建一个不包含利用细节的公开 issue，并请求私下报告渠道。

报告中建议包含：

- 受影响版本或 commit。
- 清晰的影响说明。
- 通过私下渠道提供的最小复现步骤或 proof-of-concept。
- 相关部署假设，尤其是管理端口暴露方式和 token 处理方式。

## 部署安全提示

- `AdminService` 没有调用者认证，管理端口应绑定到本机或可信管理网络。
- 妥善保管 principal token 和 ObjectKey，避免写入日志或公开输出。
- ObjectKey 是可转交的永久读取凭据，持有者即可读取对象。附加到消息后，消息读者及管理端会获得该凭据。
- 数据目录及备份含明文凭据，请限制文件访问权限，并按部署需要加密。
- 服务端使用明文 gRPC，在可信网络中运行，或通过 TLS/mTLS 代理接入。
