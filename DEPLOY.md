# IPTV 调度管理系统 - 生产环境部署手册 (v1.5)

本系统是一款专为运营商环境设计的“单文件绿色版”高性能 API 代理与 IPTV 直播流调度平台。支持 7x24 小时长效运行。

---

## 一、 核心特性
- **单文件运行**：所有 HTML 视图、静态资源（CSS/JS/图片）已全部打包进二进制文件，无需额外文件夹。
- **高性能代理**：内置 TCP 连接池，支持高并发全协议转发，自动处理路径多斜杠问题。
- **集群化管理**：内置 **MediaMTX** 一键远程部署与 HLS 态势感知监控。
- **金标准推流护城河**：推流内核深度实装 50MB 级发送套接字缓冲区 (`-buffer_size 52428800`) 与 4096 深度多路复用队列，彻底攻克高并发与高帧率（1080p 50fps）源推流下的套接字溢出与 `Broken pipe` 断连难题。
- **防风控与零延迟超大缓冲池自愈架构 (NEW)**：针对公网源断流重连真空期，将 MediaMTX 读写超时扩充至 60s、包缓冲池提升至 8192 包，HLS 切片窗保留 60s，同时推流解封装探测缩短至 5s，实现断流无缝吸收与超光速重连建连。
- **自愈初始化**：首次连接数据库自动补全唯一索引，并在用户表为空时自动生成管理员账号。
- **Panic-Free**：全链路防御性编程，数据库断开自动降级，保障进程永不崩溃。

---

## 二、 运行环境要求
- **主服务器**：Windows 7+ 或 Linux (CentOS 7+, Ubuntu 18.04+, Debian 10+)。
- **MediaMTX 节点**：推荐 Ubuntu/Debian 系统（需具备 SSH 权限，且已安装 `sudo`、`wget`、`curl`）。
- **数据库**：MongoDB 4.0+ (支持本地或云数据库)。
- **网络**：主程序默认监听 `8081` 端口，MediaMTX 节点需开放相应的 API/HLS/RTSP 端口。

---

## 三、 命令行参数说明

| 参数 | 描述 | 默认值 |
| :--- | :--- | :--- |
| `--port` | 服务监听端口 | `8081` |
| `--cmd` | 操作指令 (`run`, `start`, `stop`, `status`) | `run` |

### 指令详解：
- `run`: 在当前窗口前台运行（用于调试，查看实时日志）。
- `start`: 在后台静默运行（Windows 推荐，Linux 生产环境推荐使用 Systemd）。
- `stop`: 优雅停止后台运行的进程。
- `status`: 检查当前进程的存活状态及运行信息。

---

## 四、 Windows 部署指南

### 1. 快速启动
1. 将 `iptv.exe` 放置在独立目录。
2. 双击运行或在 CMD 中执行：`iptv.exe --port 8081 --cmd start`。
3. 访问 `http://localhost:8081`。

### 2. 初始化与账号
首次连接数据库成功后，若系统内无账号，将自动创建：
- **账号**：`admin@iptv.com`
- **密码**：`123456`

---

## 五、 Linux 生产环境部署 (Systemd 守护)

在 Linux 生产环境中，强烈建议使用 **Systemd** 进行管理，它可以确保程序在服务器重启或意外崩溃后自动拉起。

### 1. 准备工作
将编译好的 `iptv` 二进制文件上传至 `/opt/iptv/`（建议不要直接放在 root 目录下）。
```bash
mkdir -p /opt/iptv
mv iptv /opt/iptv/
chmod +x /opt/iptv/iptv
```

### 2. 创建 Systemd 服务文件
使用 root 权限创建文件 `/etc/systemd/system/iptv.service`：
```ini
[Unit]
Description=IPTV Scheduler & API Proxy Service
After=network.target mongodb.service

[Service]
# 指定运行目录，程序生成的 config.ini 会保存在此
WorkingDirectory=/opt/iptv/

# 启动命令：建议使用 run 模式由 Systemd 接管日志输出
ExecStart=/opt/iptv/iptv --port 8081 --cmd run

# 自动重启策略
Restart=always
RestartSec=5

# 日志重定向（可选，Systemd 默认会收集到 journalctl 中）
StandardOutput=append:/var/log/iptv.log
StandardError=append:/var/log/iptv.error.log

# 资源限制（防止极端情况内存泄漏影响宿主机）
MemoryLimit=512M

[Install]
WantedBy=multi-user.target
```

### 3. 启动并启用服务
```bash
# 重新加载配置文件
systemctl daemon-reload

# 启动服务
systemctl start iptv

# 设置开机自启
systemctl enable iptv

# 查看运行状态
systemctl status iptv
```

### 4. 运维常用命令
*   **停止服务**: `systemctl stop iptv`
*   **重启服务**: `systemctl restart iptv`
*   **查看实时日志**: `journalctl -u iptv -f`
*   **查看错误日志**: `tail -f /var/log/iptv.error.log`

### 5. 防火墙配置
如果无法访问，请检查防火墙是否开放了 8081 端口：
```bash
# Ubuntu/UFW
ufw allow 8081/tcp

# CentOS/Firewalld
firewall-cmd --zone=public --add-port=8081/tcp --permanent
firewall-cmd --reload
```

---

## 六、 进阶运维

### 1. 自动备份数据库
建议定期对 MongoDB 进行备份，以防数据丢失：
```bash
# 备份命令示例
mongodump --uri="mongodb://user:pass@host:port/dbname" --out /backup/iptv_$(date +%F)
```

### 2. 查看内存与协程
登录后台后，访问 `/dash/status/sys` 接口，可实时观察系统负载。
*   `alloc_mb`: 实际占用的物理内存。
*   `goroutines`: 活跃协程数，正常应保持在 50-200 之间。

---
*文档由 AI 自动生成，最后更新：2026-05-10*
