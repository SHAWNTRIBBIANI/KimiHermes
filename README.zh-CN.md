# hermes-kimi-claw（中文文档）

把自托管的 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 桥接到 **Kimi Claw**（kimi.com）的平台插件——在 Kimi App/网页里直接指挥你的 Hermes，支持打字机流式、工具卡片、思考折叠块和文件互传。

这是官方 `kimi-claw` OpenClaw 连接插件（v0.27.1）的互操作重实现。线协议从公开发布的插件包逆向整理，并经过生产环境验证——包括与 Kimi 官方云端实例真实流量的逐帧对比校准。

## 功能

- 文本/图片/文件/链接双向收发（文件在 Kimi 侧渲染为卡片）
- 打字机流式回复（WS `send-message-stream`，增量 append + 终帧对账）
- 思考折叠块、原生工具调用卡片
- 五个 Kimi 搜索系工具：`kimi_search` / `kimi_fetch` / `kimi_finance` / `kimi_datasource_get_desc` / `kimi_datasource_call`
- Kimi 群聊/多 agent 协作（kimiim-cli + 技能）
- cron 结果投递到 Kimi 会话
- 断线重连 + `sinceId` 续传、Hermes 配对鉴权
- 远程终端（agent-ws web-ssh）：已实现但默认关闭——Kimi 云端当前不对自建实例开放此通道

已知结构差异：Hermes 在工具边界会结束当前消息气泡（"一段=一个气泡"），多工具任务表现为多个依次打出的气泡，而非官方的单消息多块。内容无损。

## 安装

1. 把 `kimi-claw/` 目录复制到 `~/.hermes/plugins/`
2. 在 kimi.com → Kimi Claw →「关联已有 OpenClaw」复制安装命令，取其中的 `--bot-token km_b_prod_...`
3. 写入 `~/.hermes/.env`：`KIMI_CLAW_BOT_TOKEN=km_b_prod_...`
4. `~/.hermes/config.yaml` 增加：

   ```yaml
   platforms:
     kimi-claw:
       enabled: true
   streaming:
     enabled: true
     transport: auto
   display:
     platforms:
       kimi-claw:
         tool_progress: false
         thinking_progress: true
   ```

5. `hermes plugins enable kimi-claw` 然后重启 gateway
6. 回 Kimi 弹窗点「我已运行」，发任意消息；首次会收到配对码，执行
   `hermes pairing approve kimi-claw <配对码>` 批准

## 搜索系工具凭证

按序解析：`KIMI_PLUGIN_API_KEY` → `HERMES_CUSTOM_API_KIMI_COM_API_KEY`（现有 Kimi Code key，已验证可用）。运行时实时读取 `.env`，换 key 免重启 gateway。

## 安全注意

- bot-token 等于这个 Kimi bot 的控制权凭证，泄露即轮换
- `terminal_enabled: true` 会把本机 shell 暴露给 Kimi 会话持有者，非必要不开
- 本项目为非官方社区项目，与月之暗面无关；上游协议变更可能导致失效

## 协议文档

见 [docs/PROTOCOL.md](docs/PROTOCOL.md)：Connect-RPC 订阅、WS 流式帧编舞、文件传输、终端 envelope 的完整记录，含服务端接受/拒绝的实测结论。

## License

MIT
