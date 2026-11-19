# OpenAI 语音兼容网关

本机服务在 `127.0.0.1:18120` 兼容项目所用的腾讯 `8k_zh`、阿里 `CreateToken` 和普通语音合成。MjSIP 的现有 Java SDK 照常收发协议。真实转写使用 `gpt-live-transcribe`，合成使用 `gpt-realtime-2.1-mini`。

## 拨测入口

[打开现有 8080 拨测页](http://127.0.0.1:8080/mjsip-call-test/)。

| 新分机 | 对照 | SIP/UDP | HTTP | RTP/RTCP |
|---|---|---:|---:|---|
| [9910 · 复述](http://127.0.0.1:8080/mjsip-call-test/?route=9910) | 9900 | 15080 | 18110 | 41700–41739 |
| [9913 · 委派播报](http://127.0.0.1:8080/mjsip-call-test/?case=openai-client-single) | 9903 | 15083 | 18113 | 41800–41839 |
| [9914 · 查订单](http://127.0.0.1:8080/mjsip-call-test/?route=9914) | 9904 | 15084 | 18114 | 41900–41939 |

## 启停

1. 运行 `./scripts/build.sh -o package`。脚本使用同一工作区已有 Java 17、Maven 和依赖缓存。可用 `MAVEN_REPO_LOCAL` 指定其他缓存。
2. 首次配置把 MjSIP 项目的 `.local/openai-live.env` 完整复制到本项目的 `.local/openai-live.env`，副本权限设为 `0600`。新服务读取副本中的 `OPENAI_API_KEY` 与 `OPENAI_LIVE_HTTP_PROXY`。轮换密钥时分别更新副本和原文件，并重启对应服务。
3. 在 [拨测项目](../mjsip-call-test/) 运行 `./start-compat.command all`，或者传入一个新分机号。首次启动生成本项目的 `.local/compat.env`，供新 MjSIP 实例和协议鉴权使用。字段见 [配置模板](config/compat.env.example)。另有 `./scripts/start.sh` 可独立运行兼容网关，再用 `--existing-compat` 启动测试分机。
4. 已运行的拨测 FreeSWITCH 可用 `python3 -B reload-compat-dialplan.py` 加载新增分机。首次部署 9913 新探针时执行现有的 `install-navigation.py`，通过 Nginx 配置检查后重载。拨测页继续使用原有 8080 入口。
5. 在 `start-compat.command` 所在终端按 Ctrl+C 停止该次启动的新实例。原分机由原启动入口管理。

## 架构与数据流

```mermaid
flowchart LR
    P["浏览器 / SIP 主叫"] --> F["现有 FreeSWITCH"]
    F -->|"9900 / 9903 / 9904"| O["原 MjSIP 实例"]
    O --> V["原腾讯 / 阿里链路"]
    F -->|"9910 / 9913 / 9914"| N["三个独立 MjSIP 实例"]
    N -->|"9910 / 9914 · 腾讯兼容协议"| C["语音兼容网关 · 18120"]
    N -->|"全部新分机 · 阿里兼容协议"| C
    N -->|"9913"| L["GPT-Live / Luna"]
    E["新项目内的密钥副本"] --> C
    C <-->|"PCM 与最终转写"| A["GPT-Live-Transcribe"]
    C <-->|"文本与音频"| T["GPT-Realtime-2.1-mini"]
```

```mermaid
sequenceDiagram
    participant P as 主叫
    participant M as 新 MjSIP
    participant C as 兼容网关
    participant A as OpenAI ASR
    participant B as 业务
    participant T as OpenAI TTS
    M->>C: 腾讯 SDK 签名建连
    C->>A: 建立独立转写会话
    C-->>M: 返回就绪
    P->>M: 说话
    M->>C: 8 kHz PCM
    C-->>M: 句首事件
    C->>A: 24 kHz PCM，断句后提交
    A-->>C: 对应 item_id 的最终转写
    C-->>M: 腾讯稳态结果
    M->>B: 创建业务轮次
    B-->>M: 业务文本
    M->>C: Token 与 StartSynthesis
    C->>T: 请求朗读
    T-->>C: 流式音频
    C-->>M: 8 kHz PCM
    M-->>P: 播放
    opt 用户插话
        M->>C: 关闭旧 TTS 任务
        C->>T: 取消合成
        M->>B: 取消旧业务轮次
    end
```

## 验收入口

[查看本轮验收结果与录音](ACCEPTANCE.md)。

- `tests/media.py --ephemeral` 用实际 SDK 和 SIP/RTP 直连测试本机 OpenAI 夹具。
- `tests/media.py --ephemeral --through-switch` 另起隔离 FreeSWITCH，跑同一组桥接用例。
- `tests/live_calls.py .local/measure --routes 9900 9910 --sample order-2 --barge --rounds 30 --parallel 6` 走现有 FreeSWITCH 测云端线路，生成逐轮 JSON 和双声道 WAV，左声道为上行、右声道为返听。自动插话指标按录音间隙识别候选边界，需结合录音核对旧音与新音。实际云端拨测按账号用量计费。
- `/healthz` 返回活动 ASR／TTS 会话与事件计数。运行日志记录会话建立、提交到最终转写、首块合成 PCM 以及终态耗时。
