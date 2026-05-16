package main

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/crypto/ssh"
)

//go:embed config/mediamtx_template.yml
var mtxTemplate string

//go:embed config/mtx-snapshot-agent
var snapAgentBinary []byte

// MediaMTXNode 代表一个远程 MediaMTX 节点
type MediaMTXNode struct {
	Name        string `bson:"name" json:"name"`
	IP          string `bson:"ip" json:"ip"`
	SSHPort     string `bson:"ssh_port" json:"ssh_port"`
	SSHUser     string `bson:"ssh_user" json:"ssh_user"`
	SSHPass     string `bson:"ssh_pass" json:"ssh_pass"`
	APIPort     string `bson:"api_port" json:"api_port"`
	APIUser     string `bson:"api_user" json:"api_user"`
	APIPass     string `bson:"api_pass" json:"api_pass"`
	HLSPort     string `bson:"hls_port" json:"hls_port"`
	RTSPPort    string `bson:"rtsp_port" json:"rtsp_port"`
	MetricsPort string `bson:"metrics_port" json:"metrics_port"`
	Status      int    `bson:"status" json:"status"` // 0: 未安装, 1: 安装中, 2: 已在线, 3: 异常
	Version     string `bson:"version" json:"version"`
}

var installLogs sync.Map // map[string]chan string

// RegisterMediaMTXRoutes 注册 MediaMTX 相关的所有路由
func RegisterMediaMTXRoutes(biz *gin.RouterGroup, mtxColl *mongo.Collection) {

	// 1. 节点列表获取
	biz.GET("/mtx-nodes", func(c *gin.Context) {
		cursor, _ := mtxColl.Find(context.TODO(), bson.M{})
		var list []MediaMTXNode
		cursor.All(context.TODO(), &list)
		for i := range list {
			if list[i].SSHPass != "" {
				list[i].SSHPass = "******"
			}
		}
		c.JSON(200, list)
	})

	// 2. 节点保存/更新
	biz.POST("/mtx-nodes", func(c *gin.Context) {
		var n MediaMTXNode
		if err := c.ShouldBindJSON(&n); err != nil {
			c.JSON(200, gin.H{"status": false, "msg": "参数错误"})
			return
		}
		if n.SSHPass == "******" {
			var old MediaMTXNode
			mtxColl.FindOne(context.TODO(), bson.M{"ip": n.IP}).Decode(&old)
			n.SSHPass = old.SSHPass
		}
		_, err := mtxColl.UpdateOne(context.TODO(), bson.M{"ip": n.IP}, bson.M{"$set": n}, options.Update().SetUpsert(true))
		if err != nil {
			c.JSON(200, gin.H{"status": false, "msg": "保存失败"})
		} else {
			c.JSON(200, gin.H{"status": true})
		}
	})

	// 3. 节点删除
	biz.DELETE("/mtx-nodes", func(c *gin.Context) {
		ip := c.Query("ip")
		_, err := mtxColl.DeleteOne(context.TODO(), bson.M{"ip": ip})
		if err != nil {
			c.JSON(200, gin.H{"status": false, "msg": "删除失败"})
		} else {
			c.JSON(200, gin.H{"status": true})
		}
	})

	// 4. 远程安装 (带 SSE 日志)
	biz.POST("/mtx-install", func(c *gin.Context) {
		ip := c.Query("ip")
		var n MediaMTXNode
		var latestTag string = "v1.18.1" // 默认占位符
		mtxColl.FindOne(context.TODO(), bson.M{"ip": ip}).Decode(&n)

		go func() {
			var err error
			var out, versionCmd, host, startCmd string
			var confYaml, svc string
			var runWithLog func(string, string) error

			mtxColl.UpdateOne(context.TODO(), bson.M{"ip": ip}, bson.M{"$set": bson.M{"status": 1}})
			ch := make(chan string, 100)
			installLogs.Store(ip, ch)
			defer installLogs.Delete(ip)
			defer close(ch)

			logToFE := func(msg string) {
				select {
				case ch <- msg:
				default:
				}
			}

			logToFE(">>> 正在初始化远程环境...\n")
			host = fmt.Sprintf("%s:%s", n.IP, n.SSHPort)
			if n.SSHPort == "" {
				host = n.IP + ":22"
			}

			runWithLog = func(name, cmd string) error {
				logToFE(fmt.Sprintf(">>> 执行: %s...\n", name))
				config := &ssh.ClientConfig{
					User:            n.SSHUser,
					Auth:            []ssh.AuthMethod{ssh.Password(n.SSHPass)},
					HostKeyCallback: ssh.InsecureIgnoreHostKey(),
					Timeout:         15 * time.Second,
				}
				client, err := ssh.Dial("tcp", host, config)
				if err != nil {
					return err
				}
				defer client.Close()
				session, _ := client.NewSession()
				defer session.Close()
				stdout, _ := session.StdoutPipe()
				stderr, _ := session.StderrPipe()
				session.Start(cmd)
				go func() {
					buf := make([]byte, 1024)
					for {
						n, err := stdout.Read(buf)
						if n > 0 {
							logToFE(string(buf[:n]))
						}
						if err != nil {
							break
						}
					}
				}()
				go func() {
					buf := make([]byte, 1024)
					for {
						n, err := stderr.Read(buf)
						if n > 0 {
							logToFE(string(buf[:n]))
						}
						if err != nil {
							break
						}
					}
				}()
				return session.Wait()
			}

			// 1. 检查环境与防火墙
			err = runWithLog("环境检测与防火墙配置", fmt.Sprintf(`
				echo '%s' | sudo -S apt-get update
				echo '%s' | sudo -S apt-get install -y wget tar curl ffmpeg
				echo '>>> 检查 ffmpeg 安装路径:'
				which ffmpeg
				echo '%s' | sudo -S ufw allow %s/tcp
				echo '%s' | sudo -S ufw allow %s/tcp
				echo '%s' | sudo -S ufw allow %s/tcp
				echo '%s' | sudo -S ufw allow %s/tcp
				echo '%s' | sudo -S ufw allow 9996/tcp
			`, n.SSHPass, n.SSHPass, n.SSHPass, n.APIPort, n.SSHPass, n.HLSPort, n.SSHPass, n.RTSPPort, n.SSHPass, n.MetricsPort, n.SSHPass))
			if err != nil {
				logToFE("!!! 环境配置失败: " + err.Error())
				goto END
			}

			// 2. 动态获取最新版本并下载
			logToFE(">>> 正在查询 MediaMTX 最新发布版本...\n")
			versionCmd = "curl -s https://api.github.com/repos/bluenviron/mediamtx/releases/latest | grep '\"tag_name\":' | sed -E 's/.*\"([^\"]+)\".*/\\1/'"
			out, _ = runSSHCommand(host, n.SSHUser, n.SSHPass, versionCmd)
			latestTag = strings.TrimSpace(out)
			if latestTag == "" {
				latestTag = "v1.18.1"
			}
			logToFE(">>> 目标安装版本: " + latestTag + "\n")

			err = runWithLog("获取最新 MediaMTX 并安装", fmt.Sprintf(`
				echo '%s' | sudo -S mkdir -p /opt/mediamtx
				cd /tmp
				echo '>>> 优先通过高速镜像源 (github.tool.do) 极速拉取安装包...'
				wget --connect-timeout=10 --read-timeout=30 --tries=2 -O mediamtx.tar.gz "https://github.tool.do/bluenviron/mediamtx/releases/download/%s/mediamtx_%s_linux_amd64.tar.gz" || {
					echo '>>> 镜像源连接异常，正在尝试通过官方 GitHub 兜底拉取...'
					wget --connect-timeout=10 --read-timeout=30 --tries=2 -O mediamtx.tar.gz "https://github.com/bluenviron/mediamtx/releases/download/%s/mediamtx_%s_linux_amd64.tar.gz"
				}
				echo '%s' | sudo -S tar -xzf mediamtx.tar.gz -C /opt/mediamtx
				rm mediamtx.tar.gz
			`, n.SSHPass, latestTag, latestTag, latestTag, latestTag, n.SSHPass))
			if err != nil {
				logToFE("!!! 下载安装失败: " + err.Error())
				mtxColl.UpdateOne(context.TODO(), bson.M{"ip": ip}, bson.M{"$set": bson.M{"status": 3}})
				goto END
			}

			// 3. 生成配置 (从内嵌模板渲染)
			logToFE(">>> 正在下发全中文注释的生产级 mediamtx.yml 配置...\n")
			{
				replacer := strings.NewReplacer(
					"{{MTX_USER}}", n.APIUser,
					"{{MTX_PASS}}", n.APIPass,
					"{{MTX_API_PORT}}", n.APIPort,
					"{{MTX_RTSP_PORT}}", n.RTSPPort,
					"{{MTX_HLS_PORT}}", n.HLSPort,
					"{{MTX_METRICS_PORT}}", n.MetricsPort,
				)
				confYaml = replacer.Replace(mtxTemplate)
			}
			// 采用字符串拼接，绝对禁止对 confYaml 使用 fmt.Sprintf，防止二次污染百分号和引号
			{
				finalCmd := "sudo -S tee /opt/mediamtx/mediamtx.yml > /dev/null <<'EOF'\n" + n.SSHPass + "\n" + confYaml + "\nEOF"
				runSSHCommand(host, n.SSHUser, n.SSHPass, finalCmd)
			}

			// 4. 创建服务
			logToFE(">>> 正在挂载 MediaMTX Systemd 服务...\n")
			svc = `
[Unit]
Description=MediaMTX
After=network.target

[Service]
ExecStart=/opt/mediamtx/mediamtx /opt/mediamtx/mediamtx.yml
Restart=always
User=root

[Install]
WantedBy=multi-user.target
`
			{
				finalSvcCmd := "sudo -S tee /etc/systemd/system/mediamtx.service > /dev/null <<'EOF'\n" + n.SSHPass + "\n" + svc + "\nEOF"
				runSSHCommand(host, n.SSHUser, n.SSHPass, finalSvcCmd)
			}

			// 4.5 下发并挂载独立快照 Agent 服务
			logToFE(">>> 正在下发本地快照探活守护进程 (mtx-snapshot-agent)...\n")
			{
				config := &ssh.ClientConfig{
					User:            n.SSHUser,
					Auth:            []ssh.AuthMethod{ssh.Password(n.SSHPass)},
					HostKeyCallback: ssh.InsecureIgnoreHostKey(),
					Timeout:         30 * time.Second,
				}
				client, err := ssh.Dial("tcp", host, config)
				if err == nil {
					session, _ := client.NewSession()
					stdin, _ := session.StdinPipe()
					go func() {
						defer stdin.Close()
						stdin.Write(snapAgentBinary)
					}()
					session.Run("cat > /tmp/mtx_snap_upload")
					session.Close()

					moveCmd := fmt.Sprintf("echo '%s' | sudo -S mv /tmp/mtx_snap_upload /opt/mediamtx/mtx-snapshot-agent && echo '%s' | sudo -S chmod +x /opt/mediamtx/mtx-snapshot-agent", n.SSHPass, n.SSHPass)
					s2, _ := client.NewSession()
					s2.Run(moveCmd)
					s2.Close()
					client.Close()
				} else {
					logToFE("!!! 连接快照传输通道失败: " + err.Error() + "\n")
				}
			}

			logToFE(">>> 正在挂载快照 Agent Systemd 服务...\n")
			{
				snapSvc := `
[Unit]
Description=MediaMTX Snapshot Agent
After=network.target mediamtx.service

[Service]
WorkingDirectory=/opt/mediamtx
ExecStart=/opt/mediamtx/mtx-snapshot-agent
Restart=always
User=root

[Install]
WantedBy=multi-user.target
`
				finalSnapSvcCmd := "sudo -S tee /etc/systemd/system/mtx-snapshot.service > /dev/null <<'EOF'\n" + n.SSHPass + "\n" + snapSvc + "\nEOF"
				runSSHCommand(host, n.SSHUser, n.SSHPass, finalSnapSvcCmd)
			}

			// 5. 启动并校验
			logToFE(">>> 执行: 权限修正与启动双服务矩阵...\n")
			startCmd = fmt.Sprintf(`
				echo '%s' | sudo -S chmod +x /opt/mediamtx/mediamtx
				echo '%s' | sudo -S systemctl daemon-reload
				echo '%s' | sudo -S systemctl enable mediamtx
				echo '%s' | sudo -S systemctl restart mediamtx
				echo '%s' | sudo -S systemctl enable mtx-snapshot
				echo '%s' | sudo -S systemctl restart mtx-snapshot
				echo '>>> 实时启动状态检查:'
				echo '%s' | sudo -S systemctl status mediamtx --no-pager
				echo '%s' | sudo -S systemctl status mtx-snapshot --no-pager
			`, n.SSHPass, n.SSHPass, n.SSHPass, n.SSHPass, n.SSHPass, n.SSHPass, n.SSHPass, n.SSHPass)

			err = runWithLog("启动服务监控", startCmd)
			if err == nil {
				logToFE("\n>>> 恭喜！MediaMTX 与快照双引擎安装成功并已上线！\n")
			} else {
				logToFE("\n!!! 启动服务可能存在异常，请检查上方 Status 输出。\n")
			}

		END:
			finalStatus := 2
			if err != nil {
				finalStatus = 3
			}
			mtxColl.UpdateOne(context.TODO(), bson.M{"ip": ip}, bson.M{"$set": bson.M{"status": finalStatus, "version": latestTag}})
			time.Sleep(2 * time.Second)
		}()
		c.JSON(200, gin.H{"status": true, "msg": "安装任务已下发"})
	})

	// 5. 获取实时日志 (SSE)
	biz.GET("/mtx-install-logs", func(c *gin.Context) {
		ip := c.Query("ip")
		val, ok := installLogs.Load(ip)
		if !ok {
			c.String(404, "No active install log")
			return
		}
		ch := val.(chan string)
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Stream(func(w io.Writer) bool {
			if msg, ok := <-ch; ok {
				c.SSEvent("message", msg)
				return true
			}
			return false
		})
	})

	// 6. 服务控制
	biz.POST("/mtx-control", func(c *gin.Context) {
		ip, action := c.Query("ip"), c.Query("action")
		var n MediaMTXNode
		mtxColl.FindOne(context.TODO(), bson.M{"ip": ip}).Decode(&n)
		host := fmt.Sprintf("%s:%s", n.IP, n.SSHPort)
		if n.SSHPort == "" {
			host = n.IP + ":22"
		}

		if action == "uninstall" {
			// 卸载逻辑改为异步日志模式
			go func() {
				ch := make(chan string, 100)
				installLogs.Store(ip, ch)
				defer installLogs.Delete(ip)
				defer close(ch)

				logToFE := func(msg string) {
					select {
					case ch <- msg:
					default:
					}
				}
				runWithLog := func(name, cmd string) error {
					logToFE(fmt.Sprintf(">>> 执行: %s...\n", name))
					config := &ssh.ClientConfig{
						User: n.SSHUser, Auth: []ssh.AuthMethod{ssh.Password(n.SSHPass)},
						HostKeyCallback: ssh.InsecureIgnoreHostKey(), Timeout: 10 * time.Second,
					}
					client, err := ssh.Dial("tcp", host, config)
					if err != nil {
						return err
					}
					defer client.Close()
					session, _ := client.NewSession()
					defer session.Close()
					stdout, _ := session.StdoutPipe()
					stderr, _ := session.StderrPipe()
					session.Start(cmd)
					go func() {
						buf := make([]byte, 1024)
						for {
							n, err := stdout.Read(buf)
							if n > 0 {
								logToFE(string(buf[:n]))
							}
							if err != nil {
								break
							}
						}
					}()
					go func() {
						buf := make([]byte, 1024)
						for {
							n, err := stderr.Read(buf)
							if n > 0 {
								logToFE(string(buf[:n]))
							}
							if err != nil {
								break
							}
						}
					}()
					return session.Wait()
				}

				logToFE(">>> 正在启动 MediaMTX 与快照矩阵彻底卸载程序...\n")
				// 1. 停止并禁用双引擎服务 (增加 || true 容错，防止服务不存在时中断)
				err := runWithLog("停止并禁用服务", fmt.Sprintf("echo '%s' | sudo -S -p '' systemctl stop mediamtx || true; echo '%s' | sudo -S -p '' systemctl disable mediamtx || true; echo '%s' | sudo -S -p '' systemctl stop mtx-snapshot || true; echo '%s' | sudo -S -p '' systemctl disable mtx-snapshot || true", n.SSHPass, n.SSHPass, n.SSHPass, n.SSHPass))
				if err != nil {
					logToFE("\n!!! 停止服务指令执行异常: " + err.Error() + "\n")
				}

				// 2. 清理系统文件与目录 (使用 -v 显示详情)
				err = runWithLog("清理系统文件与目录", fmt.Sprintf("echo '%s' | sudo -S -p '' rm -fv /etc/systemd/system/mediamtx.service || true; echo '%s' | sudo -S -p '' rm -fv /etc/systemd/system/mtx-snapshot.service || true; echo '%s' | sudo -S -p '' rm -rfv /opt/mediamtx; echo '%s' | sudo -S -p '' systemctl daemon-reload", n.SSHPass, n.SSHPass, n.SSHPass, n.SSHPass))
				if err != nil {
					logToFE("\n!!! 清理文件失败: " + err.Error() + "\n")
					return
				}

				// 3. 最终确认
				logToFE(">>> 正在进行最后的物理状态校验...\n")
				err = runWithLog("校验残留情况", "ls -d /opt/mediamtx 2>&1 || echo 'CONFIRMED: Directory is gone.'")

				logToFE("\n>>> 恭喜！MediaMTX 与伴随快照服务已从服务器彻底卸载并清理完毕！\n")
				mtxColl.UpdateOne(context.TODO(), bson.M{"ip": ip}, bson.M{"$set": bson.M{"status": 0}})
				time.Sleep(2 * time.Second)
			}()
			c.JSON(200, gin.H{"status": true, "msg": "卸载任务已启动"})
			return
		}

		var cmd string
		switch action {
		case "restart":
			cmd = fmt.Sprintf("echo '%s' | sudo -S systemctl restart mediamtx && echo '%s' | sudo -S systemctl restart mtx-snapshot", n.SSHPass, n.SSHPass)
		case "stop":
			cmd = fmt.Sprintf("echo '%s' | sudo -S systemctl stop mediamtx && echo '%s' | sudo -S systemctl stop mtx-snapshot", n.SSHPass, n.SSHPass)
		}
		_, err := runSSHCommand(host, n.SSHUser, n.SSHPass, cmd)
		if err != nil {
			c.JSON(200, gin.H{"status": false, "msg": "操作失败"})
		} else {
			if action == "uninstall" {
				mtxColl.UpdateOne(context.TODO(), bson.M{"ip": ip}, bson.M{"$set": bson.M{"status": 0}})
			}
			c.JSON(200, gin.H{"status": true})
		}
	})

	// 7. API 代理
	biz.Any("/mtx-proxy/:ip/*action", func(c *gin.Context) {
		ip, action := c.Param("ip"), c.Param("action")
		var n MediaMTXNode
		mtxColl.FindOne(context.TODO(), bson.M{"ip": ip}).Decode(&n)
		targetURL := fmt.Sprintf("http://%s:%s/v3%s", n.IP, n.APIPort, action)
		if c.Request.URL.RawQuery != "" {
			targetURL += "?" + c.Request.URL.RawQuery
		}

		req, err := http.NewRequest(c.Request.Method, targetURL, c.Request.Body)
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to create request"})
			return
		}

		// 关键点：注入 Basic Auth 认证头
		req.SetBasicAuth(n.APIUser, n.APIPass)

		// 透传必要的 Header
		req.Header.Set("Content-Type", c.GetHeader("Content-Type"))
		req.Header.Set("Accept", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			c.JSON(500, gin.H{"error": "Target node unreachable"})
			return
		}
		defer resp.Body.Close()

		c.Status(resp.StatusCode)
		io.Copy(c.Writer, resp.Body)
	})

	// 8. 截图控制与静态出图代理
	biz.Any("/mtx-snap/:ip/*action", func(c *gin.Context) {
		ip, action := c.Param("ip"), c.Param("action")
		var n MediaMTXNode
		mtxColl.FindOne(context.TODO(), bson.M{"ip": ip}).Decode(&n)

		targetURL := fmt.Sprintf("http://%s:9996%s", n.IP, action)
		if c.Request.URL.RawQuery != "" {
			targetURL += "?" + c.Request.URL.RawQuery
		}

		req, err := http.NewRequest(c.Request.Method, targetURL, c.Request.Body)
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to create request"})
			return
		}
		// 只有 /api/ 前缀才注入 Basic Auth
		if strings.HasPrefix(action, "/api/") {
			req.SetBasicAuth(n.APIUser, n.APIPass)
		}
		req.Header.Set("Content-Type", c.GetHeader("Content-Type"))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			c.JSON(500, gin.H{"error": "Snapshot agent unreachable"})
			return
		}
		defer resp.Body.Close()

		c.Status(resp.StatusCode)
		io.Copy(c.Writer, resp.Body)
	})
}

// runSSHCommand 执行远程 SSH 命令
func runSSHCommand(host, user, pass string, cmd string) (string, error) {
	config := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.Password(pass)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         15 * time.Second,
	}
	client, err := ssh.Dial("tcp", host, config)
	if err != nil {
		return "", err
	}
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()
	out, err := session.CombinedOutput(cmd)
	return string(out), err
}
