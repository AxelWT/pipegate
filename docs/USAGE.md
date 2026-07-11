# PipeGate 使用指南

本指南涵盖 PipeGate 的安装、两种路由模式的使用，以及代理前端页面时静态资源 404 问题的完整解决方案。

## 目录

- [安装](#安装)
- [路径模式（默认）](#路径模式默认)
- [子域名模式（解决前端静态资源 404）](#子域名模式解决前端静态资源-404)
- [前端代理完整示例](#前端代理完整示例)
- [DNS 与 TLS 配置](#dns-与-tls-配置)
- [配置项参考](#配置项参考)
- [错误码说明](#错误码说明)
- [故障排查](#故障排查)

---

## 安装

```bash
git clone https://github.com/janbjorge/pipegate.git && cd pipegate
uv sync
```

要求 Python >= 3.12。

---

## 路径模式（默认）

connection_id 作为 URL 路径的第一段。这是开箱即用的默认行为，适合 API 代理或不含绝对路径资源的简单场景。

```bash
# 1. 设置密钥（服务端和生成 token 的机器需要相同）
export PIPEGATE_JWT_SECRET="change-me-to-something-secret"
export PIPEGATE_JWT_ALGORITHMS='["HS256"]'

# 2. 生成隧道 token（21 天有效期）
pipegate token
# 输出：
# Connection-id: a1b2c3d4...
# JWT Bearer:    eyJhbGci...

# 3. 在公网 VPS 上启动服务端
pipegate server

# 4. 在本地机器上启动客户端
pipegate client http://localhost:3000 "ws://yourserver:8000/?token=<jwt>"
```

访问方式：

```
http://yourserver:8000/a1b2c3d4/api/data  →  http://localhost:3000/api/data
```

**固定 connection_id**（推荐，这样 URL 在 token 续期后保持不变）：

```bash
# 方式一：命令行参数（单次）
pipegate token --connection-id my-app

# 方式二：环境变量（持久）
export PIPEGATE_CONNECTION_ID=my-app
pipegate token
```

命令行参数优先于环境变量。

---

## 子域名模式（解决前端静态资源 404）

### 问题

路径模式下，前端页面里的**绝对路径**资源会丢失 connection_id 前缀：

```
浏览器请求  http://server:8000/myapp/          → 返回 index.html ✓
index.html 内 <script src="/static/js/main.js">
浏览器请求  http://server:8000/static/js/main.js  → connection_id 丢失！
服务端把 "static" 当成 connection_id → 找不到隧道 → 504
```

`/assets/`、`/favicon.ico`、service worker 等所有绝对路径资源都会出现这个问题。

### 解决方案

设置 `PIPEGATE_BASE_DOMAIN` 启用子域名路由。connection_id 从 `Host` 头的最左侧标签获取，完整路径原样转发：

```
http://myapp.tunnel.example.com/             → 转发 /             到 myapp
http://myapp.tunnel.example.com/static/main.js → 转发 /static/main.js  ✓ 正常工作
```

### 使用步骤

```bash
# 服务端：启用子域名模式
export PIPEGATE_JWT_SECRET="change-me-to-something-secret"
export PIPEGATE_JWT_ALGORITHMS='["HS256"]'
export PIPEGATE_BASE_DOMAIN="tunnel.example.com"
pipegate server

# 生成固定 connection_id 的 token
pipegate token -c myapp

# 客户端连接
pipegate client http://localhost:3000 "ws://yourserver:8000/?token=<jwt>"
```

访问方式：

```
http://myapp.tunnel.example.com/              → http://localhost:3000/
http://myapp.tunnel.example.com/static/main.js → http://localhost:3000/static/main.js
```

**前端零改造** —— 无需修改 `base`、`basePath` 或重新构建。

### 两种模式对比

| 特性 | 路径模式（默认） | 子域名模式 |
|---|---|---|
| URL 形态 | `http://server/{cid}/{path}` | `http://{cid}.{base_domain}/{path}` |
| 绝对路径资源 | ✗ 404 | ✓ 正常工作 |
| 前端改造 | 需要（配 base path） | 不需要 |
| DNS 要求 | 无 | 通配 DNS `*.{base_domain}` |
| TLS 要求 | 普通证书 | 通配证书（HTTPS 时） |
| 配置项 | 无（默认） | `PIPEGATE_BASE_DOMAIN` |

> 两种模式互斥：设置 `PIPEGATE_BASE_DOMAIN` 后仅子域名模式生效；未设置时仅路径模式生效。

---

## 前端代理完整示例

以下示例展示用子域名模式代理一个 Vite + React 前端应用。

### 1. 服务端配置（VPS）

```bash
# /etc/environment 或启动脚本
export PIPEGATE_JWT_SECRET="your-long-random-secret"
export PIPEGATE_JWT_ALGORITHMS='["HS256"]'
export PIPEGATE_BASE_DOMAIN="tunnel.example.com"

pipegate server --host 0.0.0.0 --port 8000
```

### 2. DNS 配置

在 DNS 服务商添加通配 A 记录：

```
*.tunnel.example.com  A  →  你的 VPS IP
```

验证：

```bash
dig myapp.tunnel.example.com +short
# 应返回你的 VPS IP
```

### 3. 生成 token 并固定 connection_id

```bash
pipegate token -c myapp
# Connection-id: myapp
# JWT Bearer:    eyJhbGci...
```

### 4. 本地启动前端 + 隧道客户端

```bash
# 终端 1：启动本地前端开发服务器
npm run dev  # 默认 http://localhost:5173

# 终端 2：启动隧道客户端
pipegate client http://localhost:5173 "ws://yourserver:8000/?token=<jwt>"
```

### 5. 访问

浏览器打开 `http://myapp.tunnel.example.com/`，所有静态资源（`/src/main.tsx`、`/assets/*.js`、`/favicon.ico`）均正常加载。

---

## DNS 与 TLS 配置

### 通配 DNS

| DNS 服务商 | 配置方式 |
|---|---|
| Cloudflare | 添加 `*` 的 A 记录指向 VPS IP（代理状态设为 DNS only） |
| 阿里云 DNS | 添加主机记录 `*`，记录类型 A，值为 VPS IP |
| 腾讯云 DNSPod | 添加 `*` 子域名 A 记录 |
| AWS Route 53 | 添加 wildcard record `*.tunnel.example.com` |

### TLS 证书（HTTPS）

子域名模式配合 HTTPS 使用时需要通配证书。以下两种方式任选：

**方式一：Caddy 自动通配证书（推荐）**

用 Caddy 作为前置反向代理，自动申请并续期 Let's Encrypt 通配证书：

```caddyfile
# Caddyfile
*.tunnel.example.com {
    reverse_proxy localhost:8000
}
```

```bash
caddy run
```

**方式二：手动申请通配证书 + Nginx**

```bash
# 用 certbot DNS 验证申请通配证书
certbot certonly --manual --preferred-challenges dns \
  -d "*.tunnel.example.com"
```

Nginx 配置：

```nginx
server {
    listen 443 ssl;
    server_name *.tunnel.example.com;

    ssl_certificate     /etc/letsencrypt/live/tunnel.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/tunnel.example.com/privkey.pem;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

> **关键：** 反向代理必须透传原始 `Host` 头（`proxy_set_header Host $host;`），否则子域名解析会失效。

### 纯 HTTP 调试

开发调试时可不配 TLS，直接用 `http://{cid}.{base_domain}:8000/` 访问。需确保 DNS 通配记录已生效。

---

## 配置项参考

所有配置通过环境变量设置（pydantic-settings）：

| 变量 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `PIPEGATE_JWT_SECRET` | 是 | -- | JWT 签名/验证的共享密钥 |
| `PIPEGATE_JWT_ALGORITHMS` | 是 | -- | 算法列表，如 `'["HS256"]'`（不可为空） |
| `PIPEGATE_JWT_ISSUER` | 否 | `pipegate` | JWT `iss` 声明，两端必须一致 |
| `PIPEGATE_JWT_AUDIENCE` | 否 | `pipegate` | JWT `aud` 声明，两端必须一致 |
| `PIPEGATE_JWT_TTL_DAYS` | 否 | `21` | token 有效期（天） |
| `PIPEGATE_CONNECTION_ID` | 否 | 随机 UUID | 生成 token 时固定 connection_id |
| `PIPEGATE_MAX_BODY_BYTES` | 否 | 10 MB | 请求体大小上限（超出返回 413） |
| `PIPEGATE_MAX_QUEUE_DEPTH` | 否 | 100 | 单隧道队列深度（超出返回 503） |
| `PIPEGATE_BASE_DOMAIN` | 否 | -- | 启用子域名路由模式 |

---

## 错误码说明

| 状态码 | 触发条件 |
|---|---|
| `200` | 请求成功转发并返回 |
| `400` | 子域名模式下 Host 不匹配 `PIPEGATE_BASE_DOMAIN` |
| `404` | 路径模式下未提供 connection_id |
| `413` | 请求体超过 `PIPEGATE_MAX_BODY_BYTES` |
| `502` | 隧道客户端断开连接，请求未完成 |
| `503` | 隧道队列已满（客户端过慢或未连接） |
| `504` | 请求超时（5 分钟内无响应），或服务端关闭 |
| `1008` | WebSocket 连接被拒（token 缺失/过期/无效） |

---

## 故障排查

### 静态资源 404 / 504

**症状：** 代理前端页面时，HTML 能加载但 `/static/*`、`/assets/*` 等资源返回 404 或长时间等待后 504。

**原因：** 使用了路径模式，前端 HTML 中的绝对路径资源丢失了 connection_id 前缀。

**解决：** 切换到子域名模式，设置 `PIPEGATE_BASE_DOMAIN` 并配通配 DNS。详见[子域名模式](#子域名模式解决前端静态资源-404)。

### 子域名模式下返回 400

**症状：** 访问返回 `Host must be a subdomain of {base_domain}`。

**排查：**
1. 确认 DNS 通配记录已生效：`dig {cid}.{base_domain} +short`
2. 确认反向代理透传了 `Host` 头（Nginx 需 `proxy_set_header Host $host;`）
3. 确认访问的域名格式为 `{cid}.{base_domain}`，而非裸域名

### 客户端无法连接

**症状：** 客户端反复打印 `Connection failed`。

**排查：**
1. 确认服务端已启动并在监听
2. 确认 token 未过期（默认 21 天）
3. 确认 `PIPEGATE_JWT_SECRET` 和 `PIPEGATE_JWT_ALGORITHMS` 在服务端和生成 token 时一致
4. 确认 WebSocket URL 格式正确：`ws://server:8000/?token=<jwt>`

### 隧道已连接但请求 503

**症状：** 请求返回 `Queue full — tunnel client is too slow or not connected`。

**原因：** 隧道队列积压超过 `PIPEGATE_MAX_QUEUE_DEPTH`（默认 100）。

**解决：**
- 确认本地服务响应正常，没有卡死
- 适当调大队列深度：`export PIPEGATE_MAX_QUEUE_DEPTH=500`

### 请求超时返回 504

**症状：** 请求等待 5 分钟后返回 504。

**原因：** 本地服务未在超时时间内响应，或隧道客户端无法访问本地服务。

**解决：**
- 确认本地服务已启动且监听地址正确
- 确认 `pipegate client` 的 target_url 可达（如 `http://localhost:3000`）
- 检查本地服务是否有防火墙限制

---

## CLI 命令速查

```
pipegate token [-c ID]              生成 JWT bearer token
pipegate client TARGET_URL WS_URL   启动隧道客户端
pipegate server [--host H] [-p N]   启动服务端（默认 0.0.0.0:8000）
```

| 命令 | 参数 | 说明 |
|---|---|---|
| `token` | `-c, --connection-id ID` | 固定 connection_id（覆盖环境变量） |
| `client` | `TARGET_URL` | 本地服务地址，如 `http://localhost:3000` |
| `client` | `WS_URL` | 服务端 WebSocket 地址，含 `?token=<jwt>` |
| `server` | `--host H` | 监听地址（默认 `0.0.0.0`） |
| `server` | `-p, --port N` | 监听端口（默认 `8000`） |
