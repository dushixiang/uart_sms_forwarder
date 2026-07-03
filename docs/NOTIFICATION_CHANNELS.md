# 通知渠道配置指南

本文档详细说明如何配置 UART SMS Forwarder 的各种通知渠道。

## 支持的通知渠道

- ✅ **钉钉** (DingTalk)
- ✅ **企业微信** (WeChat Work)
- ✅ **飞书** (Feishu)
- ✅ **Telegram**
- ✅ **邮箱** (Email)
- ✅ **自定义Webhook** (Custom Webhook)
- ✅ **Bark** (iOS 推送) - 新增
- ✅ **Gotify** (自建推送) - 新增

## 1. Bark 推送通知

### 什么是 Bark？

Bark 是一个 iOS 推送通知应用，支持自建服务器。可以从任何来源接收消息并在 iOS 设备上推送通知。

### 配置步骤

#### 1.1 iOS 设备准备
1. 在 App Store 搜索 "Bark" 下载应用
2. 打开应用记录 **Device Key**（设备密钥）
3. 可选：自建 Bark 服务器或使用官方服务

#### 1.2 自建 Bark 服务器（可选）

```bash
# 使用 Docker 部署
docker run -d \
  --name bark \
  -p 8080:8080 \
  -e LOG_LEVEL=info \
  finab/bark-server:latest
```

> 访问 https://github.com/Finb/bark-server 了解更多

#### 1.3 在 UART SMS Forwarder 中配置

**API 调用示例：**

```bash
curl -X PUT http://localhost:8080/api/properties/notification_channels \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "name": "通知渠道配置",
    "value": [
      {
        "type": "bark",
        "enabled": true,
        "config": {
          "serverUrl": "https://api.day.app",  # 或自建服务器地址
          "deviceKey": "YOUR_DEVICE_KEY_HERE"
        }
      }
    ]
  }'
```

**配置参数说明：**

| 参数 | 说明 | 示例 |
|------|------|------|
| `serverUrl` | Bark 服务器地址 | `https://api.day.app` |
| `deviceKey` | 设备密钥（从 Bark 应用获取） | `abc123xyz...` |

#### 1.4 测试

```bash
curl -X POST http://localhost:8080/api/notifications/bark/test \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

---

## 2. Gotify 推送通知

### 什么是 Gotify？

Gotify 是一个简单的、轻量级的、自托管的推送通知服务。支持 Web 界面和移动应用。

### 配置步骤

#### 2.1 部署 Gotify 服务器

**使用 Docker Compose：**

```yaml
# docker-compose.yml
version: '3'
services:
  gotify:
    image: gotify/gotify-server:latest
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - gotify_data:/var/gotify
    environment:
      GOTIFY_SERVER_PORT: 80
      GOTIFY_DEFAULTUSER_PASS: "admin"  # 默认密码，请修改

volumes:
  gotify_data:
```

启动：
```bash
docker-compose up -d
```

> 访问 https://gotify.net 了解更多详情

#### 2.2 获取 Token

1. 访问 `http://your-gotify-server`
2. 用户名: `admin` 密码: 默认密码
3. 进入 **Apps** 页面，创建新应用
4. 复制生成的 **Token**

#### 2.3 在 UART SMS Forwarder 中配置

**API 调用示例：**

```bash
curl -X PUT http://localhost:8080/api/properties/notification_channels \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "name": "通知渠道配置",
    "value": [
      {
        "type": "gotify",
        "enabled": true,
        "config": {
          "serverUrl": "http://your-gotify-server",
          "token": "YOUR_APP_TOKEN_HERE"
        }
      }
    ]
  }'
```

**配置参数说明：**

| 参数 | 说明 | 示例 |
|------|------|------|
| `serverUrl` | Gotify 服务器地址 | `http://192.168.1.100:80` |
| `token` | 应用 Token（从 Gotify 后台获取） | `AgxxxxxxxxxQ` |

#### 2.4 测试

```bash
curl -X POST http://localhost:8080/api/notifications/gotify/test \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

---

## 3. 钉钉配置

### 3.1 获取 Webhook 信息

1. 打开钉钉，创建群组
2. 右上角菜单 → **管理** → **智能群助手** → **添加机器人**
3. 选择 **自定义机器人**
4. 复制 **Webhook URL** 中的 `access_token`

### 3.2 配置示例

```bash
curl -X PUT http://localhost:8080/api/properties/notification_channels \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "name": "通知渠道配置",
    "value": [
      {
        "type": "dingtalk",
        "enabled": true,
        "config": {
          "secretKey": "WEBHOOK_ACCESS_TOKEN",
          "signSecret": "OPTIONAL_SIGN_SECRET"  # 可选
        }
      }
    ]
  }'
```

---

## 4. 企业微信配置

### 4.1 获取 Webhook

1. 打开企业微信管理后台
2. **应用管理** → **创建应用** → 选择 **创建内部应用**
3. 复制 **Webhook URL** 中的 `key` 值

### 4.2 配置示例

```bash
curl -X PUT http://localhost:8080/api/properties/notification_channels \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "name": "通知渠道配置",
    "value": [
      {
        "type": "wecom",
        "enabled": true,
        "config": {
          "secretKey": "WEBHOOK_KEY"
        }
      }
    ]
  }'
```

---

## 5. 飞书配置 (修复了加签 BUG)

### 5.1 获取 Webhook

1. 打开飞书
2. **应用商店** → 搜索 **自定义机器人**
3. 创建机器人，复制 **Webhook URL**

### 5.2 配置示例

```bash
curl -X PUT http://localhost:8080/api/properties/notification_channels \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "name": "通知渠道配置",
    "value": [
      {
        "type": "feishu",
        "enabled": true,
        "config": {
          "secretKey": "WEBHOOK_SECRET_KEY",
          "signSecret": "OPTIONAL_SIGN_SECRET"  # 可选，用于加签
        }
      }
    ]
  }'
```

> **注意**：飞书加签 BUG 已修复，现在正确使用 HMAC-SHA256

---

## 6. Telegram 配置

### 6.1 获取 Bot Token

1. 打开 Telegram，搜索 `@BotFather`
2. 发送 `/newbot` 创建机器人
3. 复制生成的 **Token**

### 6.2 获取 Chat ID

```bash
# 发送消息给 bot，然后执行
curl "https://api.telegram.org/botYOUR_BOT_TOKEN/getUpdates"

# 找到 "chat": { "id": CHAT_ID }
```

### 6.3 配置示例

```bash
curl -X PUT http://localhost:8080/api/properties/notification_channels \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "name": "通知渠道配置",
    "value": [
      {
        "type": "telegram",
        "enabled": true,
        "config": {
          "apiToken": "YOUR_BOT_TOKEN",
          "userid": "YOUR_CHAT_ID",
          "proxyEnabled": false,
          "proxyUrl": "",
          "proxyUsername": "",
          "proxyPassword": ""
        }
      }
    ]
  }'
```

---

## 7. 邮箱配置

### 7.1 配置示例

```bash
curl -X PUT http://localhost:8080/api/properties/notification_channels \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "name": "通知渠道配置",
    "value": [
      {
        "type": "email",
        "enabled": true,
        "config": {
          "smtpHost": "smtp.gmail.com",
          "smtpPort": "587",
          "username": "your-email@gmail.com",
          "password": "YOUR_APP_PASSWORD",
          "from": "sender@example.com",
          "to": "recipient@example.com",
          "subject": "新短信 - {{from}}"
        }
      }
    ]
  }'
```

### 7.2 常见 SMTP 服务器

| 服务 | 主机 | 端口 | 是否需要密码 |
|------|------|------|-----------|
| Gmail | smtp.gmail.com | 587 | 是（应用密码） |
| QQ 邮箱 | smtp.qq.com | 587 | 是（授权码） |
| 网易邮箱 | smtp.163.com | 587 | 是（授权码） |
| 阿里邮箱 | smtp.aliyun.com | 465 | 是 |

---

## 8. 自定义 Webhook 配置

### 8.1 配置示例

```bash
curl -X PUT http://localhost:8080/api/properties/notification_channels \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "name": "通知渠道配置",
    "value": [
      {
        "type": "webhook",
        "enabled": true,
        "config": {
          "url": "https://your-api.com/webhook",
          "method": "POST",
          "contentType": "application/json",
          "body": "{\"text\": \"{{type}} - {{from}}: {{content}}\"}",
          "headers": {
            "Authorization": "Bearer YOUR_TOKEN",
            "X-Custom-Header": "value"
          }
        }
      }
    ]
  }'
```

### 8.2 模板变量

- `{{from}}` - 来源号码
- `{{content}}` - 消息内容
- `{{type}}` - 消息类型（sms 或 call）
- `{{timestamp}}` - 时间戳

---

## 完整配置示例

将所有通知渠道配置为一个 JSON 数组：

```bash
curl -X PUT http://localhost:8080/api/properties/notification_channels \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "name": "通知渠道配置",
    "value": [
      {
        "type": "bark",
        "enabled": true,
        "config": {
          "serverUrl": "https://api.day.app",
          "deviceKey": "YOUR_DEVICE_KEY"
        }
      },
      {
        "type": "gotify",
        "enabled": true,
        "config": {
          "serverUrl": "http://192.168.1.100",
          "token": "YOUR_GOTIFY_TOKEN"
        }
      },
      {
        "type": "dingtalk",
        "enabled": true,
        "config": {
          "secretKey": "YOUR_DINGTALK_TOKEN"
        }
      },
      {
        "type": "wecom",
        "enabled": true,
        "config": {
          "secretKey": "YOUR_WECOM_TOKEN"
        }
      },
      {
        "type": "feishu",
        "enabled": true,
        "config": {
          "secretKey": "YOUR_FEISHU_TOKEN",
          "signSecret": "YOUR_SIGN_SECRET"
        }
      },
      {
        "type": "email",
        "enabled": true,
        "config": {
          "smtpHost": "smtp.gmail.com",
          "smtpPort": "587",
          "username": "your-email@gmail.com",
          "password": "YOUR_APP_PASSWORD",
          "from": "sender@example.com",
          "to": "recipient@example.com"
        }
      }
    ]
  }'
```

---

## 测试通知渠道

### 测试单个渠道

```bash
curl -X POST http://localhost:8080/api/notifications/{channel_type}/test \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

支持的 channel_type：
- `bark`
- `gotify`
- `dingtalk`
- `wecom`
- `feishu`
- `email`
- `telegram`
- `webhook`

### 示例

```bash
# 测试 Bark
curl -X POST http://localhost:8080/api/notifications/bark/test \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"

# 测试 Gotify
curl -X POST http://localhost:8080/api/notifications/gotify/test \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

---

## 故障排除

### Bark

| 问题 | 原因 | 解决方案 |
|------|------|--------|
| 无法连接服务器 | 网络问题或 URL 错误 | 检查服务器地址和网络连接 |
| Device Key 无效 | 密钥过期或错误 | 在 Bark 应用中重新获取 |
| 推送未到达 | 设备离线或应用关闭 | 确保 Bark 应用在运行 |

### Gotify

| 问题 | 原因 | 解决方案 |
|------|------|--------|
| Token 无效 | Token 过期或删除 | 在 Gotify 后台重新生成 Token |
| 连接超时 | 服务器地址错误 | 确保服务器可访问 |
| 推送未显示 | 优先级设置 | 调整优先级配置 |

### 飞书加签失败

**已修复**：确保使用最新版本的 notifier.go，加签计算现在正确使用 HMAC-SHA256。

---

## 最佳实践

1. **配置多个通知渠道** - 提供冗余和备份
2. **定期测试** - 使用测试端点验证配置有效性
3. **保护敏感信息** - Token 和密码应存储在安全位置
4. **监控日志** - 检查应用日志了解通知发送状态
5. **错误处理** - 配置失败时检查错误消息

---

## 更新日志

### v2.0.0 (新增功能)

✅ 新增 **Bark** 推送通知支持
✅ 新增 **Gotify** 自建推送支持
✅ 修复飞书加签 BUG (HMAC-SHA256)
✅ 改进 Telegram 配置验证
✅ 增强错误处理和日志记录

---

## 获取帮助

- GitHub Issues: https://github.com/dushixiang/uart_sms_forwarder/issues
- 项目文档: https://blog.typesafe.cn/posts/air780e-giffgaff/

