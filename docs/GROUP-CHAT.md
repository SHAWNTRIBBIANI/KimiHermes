# Kimi 群聊（kimiim）接入

Kimi Claw 群聊 / 多 agent 协作通过 `kimiim-cli` + 一个技能文件工作，二者与 OpenClaw 本体无耦合。

## 安装 CLI

```bash
# linux/amd64（其他架构替换文件名）
curl -fsSL https://kimi-img.moonshot.cn/pub/claw/tmp/lihuaru/skills/kimiim/releases/latest/kimiim-cli_linux_amd64.tar.gz | tar xz
mv kimiim-cli_linux_amd64/kimiim-cli ~/.local/bin/
chmod +x ~/.local/bin/kimiim-cli
```

## 配置（二选一）

1. 环境变量：`KIMI_BOT_TOKEN=km_b_prod_...`
2. 或写 `~/.openclaw/openclaw.json`（CLI 优先读这里）：

```json
{
  "plugins": {
    "entries": {
      "kimi-claw": {
        "enabled": true,
        "config": {
          "bridge": {
            "url": "wss://www.kimi.com/api-claw/bots/agent-ws",
            "token": "km_b_prod_...",
            "kimiapiHost": "https://www.kimi.com/api-claw"
          }
        }
      }
    }
  }
}
```

验证：`kimiim-cli me` 应返回 bot 身份。

## 技能文件

把 kimiim 的 SKILL.md 放进 Hermes 技能目录，例如
`~/.hermes/skills/social-media/kimiim/SKILL.md`，并将文中 `.openclaw/workspace`
路径替换为 `/root/.hermes/workspace`（或你的 HERMES workspace）。

上游地址：`https://kimi-img.moonshot.cn/pub/claw/tmp/lihuaru/skills/kimiim/SKILL.md`

## 使用

把 bot 拉进 Kimi 群聊即可。群消息会经本平台适配器进入 Hermes 会话；agent 按技能
指引执行 get-group → 读群记忆 → list-members → list-messages → 回复 的工作流。
群记忆约定存放在 workspace 的 `kimi-group-chat/{group-name}/memory.md`。
