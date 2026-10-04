# 0006: 极端边界安全防线、大报文截断熔断与只读预检开放

针对在企业级生产落地中可能遭遇的极端运维操作越界、超大载荷撑爆大模型上下文窗口、证书配置静默旁路以及只读集群无法安全评估 Lag 等实际问题，我们决定对系统进行深度安全加固与性能演进。

## 决策背景

1. **绝对位移重置越界风险 (Future Offset Vulnerability)**：
   在 `kafka_reset_consumer_group_offsets` 的 `to_offset` 策略中，此前仅实施了 `max(0, offset)` 的下界防御，未校验分区当前日志末端位移 (Log End Offset, LEO)。若调用方传入超出当前最新位移的未来数值，底层提交成功后将导致后续所有正常业务写入全部被消费者永久跳过，造成灾难性数据静默丢失。
2. **凭据路径不存在时的静默旁路 (Silent Bypass)**：
   在 SSL/mTLS 配置构建中，若用户显式指定了 `ssl_cafile`、`ssl_certfile` 或 `ssl_keyfile`，但因路径书写错误或权限不足导致文件不存在时，系统静默回退并跳过证书加载，导致排查 TLS 握手失败异常困难，违背 Fail-Fast 原则。
3. **超大载荷撑爆上下文 (Payload Overflow)**：
   在采样拉取消息时，若单条消息载荷达到数十 MB（如大图 Base64 或全量大 JSON），直接解码透传会导致 MCP 客户端界面卡死或撑爆大语言模型的上下文窗口。
4. **只读模式阻断安全预检 (Read-Only Blind Spot)**：
   此前在全局 `--read-only` 模式下直接隐藏了位移重置工具。然而运维与巡检人员在只读生产环境下正需要借助其安全的 `dry_run=True` 能力生成评估报告，过度隐藏导致只读模式下无法评估 Lag 调整影响。
5. **消费组多主题查询 N+1 性能瓶颈**：
   在解析涉足主题物理分区时，在循环体内逐个查询主题，产生了典型的 N+1 串行 RPC 网络往返开销。

## 决策内容

1. **位移上界约束与原子性拦截 (Offset Upper Bound Guard)**：
   - 在 `to_offset` 策略计算阶段，预先拉取涉及分区的 `end_offsets` (LEO)；
   - 实行**整体验证原子性拦截**：只要任意目标分区的 `offset > LEO`，立即整体中止并抛出越界异常，拒绝生成任何生效计划，明确返回越界分区及当前 LEO 边界。

2. **凭据配置严格 Fail-Fast 校验**：
   - 在配置解析与 `build_ssl_context` 阶段，对显式配置的 `ssl_cafile`、`ssl_certfile`、`ssl_keyfile` 进行严格存在性检查；
   - 若文件不存在或无读取权限，直接抛出语义明确的 `FileNotFoundError`；配置客户端证书时强制成对校验私钥文件。

3. **消息截断熔断保护 (Message Truncation Guard)**：
   - 在 `decode_payload` 与 `kafka_sample_messages` 中增加可配置参数 `max_bytes_per_message: int = 65536`（默认 64KB）；
   - 当单条消息载荷字节数超过阈值时，自动实施前缀截断，安全降级为截断纯文本字符串，并附带 `truncated: True` 与 `original_size_bytes` 元数据，杜绝撑爆模型上下文与半截 JSON 解析异常。

4. **只读模式下放开 Dry-Run 预检与动态元数据注入**：
   - 在只读模式（全局只读或连接级只读）下，允许注册 `kafka_reset_consumer_group_offsets` 工具；
   - 动态在工具描述前缀注入 `【只读模式：仅支持 dry_run=True 预检评估，物理提交将被严格拦截】` 引导模型；
   - 执行器强制校验：在只读模式下若调用方尝试传入 `dry_run=False`，立即拦截并阻断。

5. **涉足主题单次批量 RPC 拓扑解析**：
   - 将 `describe_consumer_group` 中对涉足主题的分区查询重构为单次批量 `admin.describe_topics(list(involved_topics))` 调用，单次网络往返完成全部分区拓扑对齐。
