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

- 不要把 `AdminService` 直接暴露到公网。
- 管理端口应绑定到本机或可信管理网络。
- 避免在日志、shell 历史、CI 输出、trace 和 issue 中泄露 principal token 和对象 token。
- ObjectKey 是永久、可转交的 bearer credential。任何持有 object ID 和 token 的调用方都能在没有
  principal 认证的情况下读取对象；当前没有撤销、轮换或删除操作。
- 把 ObjectKey 附加到消息会向所有有权读取该消息的调用方以及 AdminService ListMessages 披露它；
  应据此设置 Channel ACL。
- RocksDB 明文保存 principal token 和对象 token。限制对 `storage.path` 的文件系统访问，按需要
  加密存储和备份，并把备份作为包含凭据的敏感数据处理。
- 参考服务端使用不加密的 gRPC credentials。业务和管理端口必须处于可信网络边界之后，或由符合
  部署要求的 TLS/mTLS 代理保护。
