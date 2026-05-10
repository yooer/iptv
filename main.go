package main

import (
	"context"
	"crypto/md5"
	"embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ProcessInfo 用于 PID 文件记录和状态回显
type ProcessInfo struct {
	Pid     int       `json:"pid"`
	Port    string    `json:"port"`
	StartAt time.Time `json:"start_at"`
}

const AppVersion = "26.05.10.104335"

const NavbarTemplate = `
<div class="nav-links">
    <a href="/dash/api"    class="nav-link {{ACTIVE_API}}">接口代理</a>
    <a href="/dash/m3u8"   class="nav-link {{ACTIVE_M3U8}}">直播调度</a>
    <a href="/dash/menu"   class="nav-link {{ACTIVE_MENU}}">节目映射</a>
    <a href="/dash/users"  class="nav-link {{ACTIVE_USERS}}">用户管理</a>
    <a href="/dash/system" class="nav-link {{ACTIVE_SYSTEM}}">系统设置</a>
</div>`

//go:embed views/*.html
//go:embed static/*
var embeddedFS embed.FS

// --- 数据模型定义 ---

// APIBackend 代表一个代理后端节点
type APIBackend struct {
	Name   string `bson:"name" json:"name"`     // 节点名称
	IP     string `bson:"ip" json:"ip"`         // 后端地址 (URL)
	State  bool   `bson:"state" json:"state"`   // 是否在线状态
	Active bool   `bson:"active" json:"active"` // 是否为当前活动的代理节点
}

// M3u8Source 代表一个直播源服务器
type M3u8Source struct {
	Name   string `bson:"name" json:"name"`     // 源名称
	Type   string `bson:"type" json:"type"`     // 类型: mistserver / tenglong
	IP     string `bson:"ip" json:"ip"`         // 服务器 IP
	Port   string `bson:"port" json:"port"`     // 服务端口
	Active bool   `bson:"active" json:"active"` // 是否为当前活动的调度源
	Weight int    `bson:"weight" json:"weight"` // 权重 1-10
}

// MenuMapping 代表节目单 ID 到名称的映射
type MenuMapping struct {
	ID     string `bson:"id" json:"id"`         // 节目 ID
	Name   string `bson:"name" json:"name"`     // 节目名称
	Sort   int    `bson:"sort" json:"sort"`     // 排序
	Active bool   `bson:"active" json:"active"` // 开启状态
}

// Member 代表管理后台的用户
type Member struct {
	Mail     string `bson:"mail" json:"mail"`         // 邮箱 (登录账号)
	Password string `bson:"password" json:"password"` // MD5 加密后的密码
	Level    int    `bson:"level" json:"level"`       // 权限等级 (= 100 为有效)
	Name     string `bson:"name" json:"name"`         // 用户姓名
}

// Config 代表系统外部配置文件结构
type Config struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Pass     string `json:"pass"`
	DBName   string `json:"dbname"`
	MongoURI string `json:"mongo_uri"`
}

// --- 全局变量与配置 ---

var (
	globalConfig Config
	dbConnected  bool
	mongoClient  *mongo.Client // 全局 client 引用，便于重连时关闭旧连接

	memberColl *mongo.Collection
	apiColl    *mongo.Collection
	m3u8Coll   *mongo.Collection
	menuColl   *mongo.Collection

	// 内存缓存
	cacheBackends []APIBackend
	cacheSources  []M3u8Source
	cacheLock     sync.RWMutex

	// 内存鉴权存储 (Session with TTL)
	sessions = make(map[string]SessionInfo) // key: dreamId, value: SessionInfo
	sessLock sync.RWMutex

	// 全局 HTTP 客户端 (带连接池，防止长周期运行产生大量 TIME_WAIT)
	httpClient *http.Client
)

// loadConfig 自动加载或生成当前目录下的 config.json
func loadConfig() {
	file, err := os.ReadFile("config.ini")
	if err != nil {
		// 默认配置
		globalConfig = Config{
			Host:   "127.0.0.1",
			Port:   "27017",
			User:   "",
			Pass:   "",
			DBName: "iptv_db",
		}
		if globalConfig.User == "" {
			globalConfig.MongoURI = fmt.Sprintf("mongodb://%s:%s/%s", globalConfig.Host, globalConfig.Port, globalConfig.DBName)
		} else {
			globalConfig.MongoURI = fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", globalConfig.User, globalConfig.Pass, globalConfig.Host, globalConfig.Port, globalConfig.DBName)
		}

		data, _ := json.MarshalIndent(globalConfig, "", "  ")
		os.WriteFile("config.ini", data, 0644)
		log.Println("[配置] 已在当前目录生成默认 config.ini")
	} else {
		json.Unmarshal(file, &globalConfig)
		// 确保 URI 总是最新的
		if globalConfig.User == "" {
			globalConfig.MongoURI = fmt.Sprintf("mongodb://%s:%s/%s", globalConfig.Host, globalConfig.Port, globalConfig.DBName)
		} else {
			globalConfig.MongoURI = fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", globalConfig.User, globalConfig.Pass, globalConfig.Host, globalConfig.Port, globalConfig.DBName)
		}
		log.Println("[配置] 成功加载 config.ini")
	}
}

// loadCache 从数据库同步数据到内存缓存
func loadCache() {
	if !dbConnected || apiColl == nil || m3u8Coll == nil {
		return
	}
	cacheLock.Lock()
	defer cacheLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 缓存后端代理集群
	cursor, err := apiColl.Find(ctx, bson.M{})
	if err == nil && cursor != nil {
		var list []APIBackend
		if err := cursor.All(ctx, &list); err == nil {
			cacheBackends = list
		}
		cursor.Close(ctx)
	}

	// 缓存直播调度服务器
	cursor, err = m3u8Coll.Find(ctx, bson.M{})
	if err == nil && cursor != nil {
		var list []M3u8Source
		if err := cursor.All(ctx, &list); err == nil {
			cacheSources = list
		}
		cursor.Close(ctx)
	}

	log.Printf("[缓存] 内存配置已同步 (代理节点: %d, 调度节点: %d)", len(cacheBackends), len(cacheSources))
}

// initDB 初始化 MongoDB 连接（支持热重连：先断开旧连接再重连）
func initDB() {
	// 断开旧连接（避免连接泄漏）
	if mongoClient != nil {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = mongoClient.Disconnect(disconnectCtx)
		cancel()
		mongoClient = nil
		dbConnected = false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(globalConfig.MongoURI))
	if err != nil {
		log.Printf("[错误] MongoDB 连接初始化失败: %v", err)
		dbConnected = false
		return
	}

	// 探测连接
	if err := client.Ping(ctx, nil); err != nil {
		log.Printf("[错误] MongoDB 无法连通: %v", err)
		dbConnected = false
		return
	}

	mongoClient = client
	dbConnected = true
	db := client.Database(globalConfig.DBName)
	memberColl = db.Collection("Members")
	apiColl = db.Collection("proxy_api")
	m3u8Coll = db.Collection("proxy_m3u8")
	menuColl = db.Collection("proxy_menu")

	// 初始化加载缓存
	loadCache()

	// 确保数据库索引和默认管理员账号存在
	ensureDatabaseDefaults()
}

// ensureDatabaseDefaults 确保数据库索引和默认管理员账号存在
func ensureDatabaseDefaults() {
	if !dbConnected || memberColl == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. 创建唯一索引 (如果已存在则跳过)
	uniqueOpt := options.Index().SetUnique(true)

	// Members: mail 唯一
	_, _ = memberColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "mail", Value: 1}},
		Options: uniqueOpt,
	})

	// proxy_api: ip 唯一
	_, _ = apiColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "ip", Value: 1}},
		Options: uniqueOpt,
	})

	// proxy_m3u8: ip 唯一
	_, _ = m3u8Coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "ip", Value: 1}},
		Options: uniqueOpt,
	})

	// proxy_menu: id 唯一
	_, _ = menuColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "id", Value: 1}},
		Options: uniqueOpt,
	})

	// 2. 检查并创建默认管理员
	count, err := memberColl.CountDocuments(ctx, bson.M{})
	if err == nil && count == 0 {
		log.Println("[数据库] 检测到用户表为空，正在创建默认管理员账号...")
		// 123456 的前端 MD5 为 e10adc3949ba59abbe56e057f20f883e
		defaultPass := passwd("e10adc3949ba59abbe56e057f20f883e")
		_, err = memberColl.InsertOne(ctx, Member{
			Mail:     "admin@iptv.com",
			Password: defaultPass,
			Level:    100,
			Name:     "管理员",
		})
		if err == nil {
			log.Println("[数据库] 默认管理员创建成功: admin@iptv.com / 123456")
		} else {
			log.Printf("[数据库] 创建默认管理员失败: %v", err)
		}
	}
}

// passwd 对原始密码进行两次 MD5 加盐加密
func passwd(raw string) string {
	m1 := md5.Sum([]byte(raw))
	m2 := md5.Sum([]byte(raw + hex.EncodeToString(m1[:])))
	return hex.EncodeToString(m2[:])
}

// SessionInfo 带过期时间的 Session 数据
type SessionInfo struct {
	Mail     string
	ExpireAt time.Time
}

const sessionTTL = 24 * time.Hour // Session 有效期：1 天

// getValidSession 获取有效 session（检查过期）
func getValidSession(token string) (string, bool) {
	if token == "" {
		return "", false
	}
	sessLock.RLock()
	info, ok := sessions[token]
	sessLock.RUnlock()
	if !ok {
		return "", false
	}
	if time.Now().After(info.ExpireAt) {
		// 过期：删除
		sessLock.Lock()
		delete(sessions, token)
		sessLock.Unlock()
		return "", false
	}
	return info.Mail, true
}

// authMiddleware API 鉴权中间件：未登录返回 401 JSON
func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("dreamId")
		if token == "" {
			token, _ = c.Cookie("dreamId")
		}
		mail, ok := getValidSession(token)
		if !ok {
			log.Printf("[鉴权失败] Token: %s", token)
			c.JSON(401, gin.H{"status": false, "msg": "认证已失效，请重新登录管理后台"})
			c.Abort()
			return
		}
		c.Set("userMail", mail)
		c.Next()
	}
}

// pageAuthMiddleware 页面鉴权中间件：未登录 302 跳转到登录页
func pageAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("dreamId")
		if token == "" {
			token, _ = c.Cookie("dreamId")
		}
		mail, ok := getValidSession(token)
		if !ok {
			c.Redirect(302, "/dash/login")
			c.Abort()
			return
		}
		c.Set("userMail", mail)
		c.Next()
	}
}

// startSessionCleaner 启动后台 goroutine，每小时清理过期 Session
func startSessionCleaner() {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			sessLock.Lock()
			before := len(sessions)
			for token, info := range sessions {
				if now.After(info.ExpireAt) {
					delete(sessions, token)
				}
			}
			after := len(sessions)
			sessLock.Unlock()
			if before != after {
				log.Printf("[Session] 已清理 %d 个过期会话，当前剩余 %d 个", before-after, after)
			}
		}
	}()
}

func main() {
	cmd := flag.String("cmd", "run", "执行指令: start|stop|restart|status|help")
	port := flag.String("port", "8081", "监听端口")
	flag.Parse()

	pidFile := "server.pid"

	switch *cmd {
	case "help":
		fmt.Printf("IPTV 调度管理中心 v%s\n", AppVersion)
		fmt.Printf("用法: %s --cmd [start|stop|restart|status|help] --port [8081]\n", os.Args[0])
		return
	case "status":
		data, err := os.ReadFile(pidFile)
		if err != nil {
			fmt.Println("状态: [已停止] (未发现运行记录)")
			return
		}
		var info ProcessInfo
		json.Unmarshal(data, &info)

		// 检查进程是否真实存活
		process, err := os.FindProcess(info.Pid)
		isAlive := err == nil
		if runtime.GOOS != "windows" && isAlive {
			// Unix 下 FindProcess 总是成功，需发送信号 0 探测
			err = process.Signal(syscall.Signal(0))
			isAlive = (err == nil)
		}

		if !isAlive {
			fmt.Printf("状态: [异常] (PID %d 已失效，但文件未清理)\n", info.Pid)
			return
		}

		fmt.Printf("--- 运营商级系统监控 (v%s) ---\n", AppVersion)
		fmt.Printf("进程状态: [运行中]\n")
		fmt.Printf("进程 PID: %d\n", info.Pid)
		fmt.Printf("监听端口: %s\n", info.Port)

		uptime := time.Since(info.StartAt).Round(time.Second)
		fmt.Printf("运行时间: %v (启动于 %s)\n", uptime, info.StartAt.Format("2006-01-02 15:04:05"))

		// 尝试通过本地 API 获取实时内存
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1%s/dash/status/sys", info.Port))
		if err == nil {
			defer resp.Body.Close()
			var sysInfo map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&sysInfo)
			fmt.Printf("内存分配: %v MiB\n", sysInfo["alloc_mb"])
			fmt.Printf("系统占用: %v MiB\n", sysInfo["sys_mb"])
			fmt.Printf("协程总数: %v\n", sysInfo["goroutines"])
		} else {
			fmt.Println("内存信息: [无法获取] (服务可能繁忙或 API 受限)")
		}
		return
	case "stop":
		data, err := os.ReadFile(pidFile)
		if err != nil {
			fmt.Println("停止失败: 未发现运行中的 PID 文件")
			return
		}
		var info ProcessInfo
		if err := json.Unmarshal(data, &info); err != nil {
			// 兼容旧的纯文本 PID 格式
			pidStr := strings.TrimSpace(string(data))
			fmt.Sscanf(pidStr, "%d", &info.Pid)
		}
		if info.Pid == 0 {
			fmt.Println("无法解析 PID 记录")
			return
		}

		pidStr := fmt.Sprintf("%d", info.Pid)
		fmt.Printf("正在停止 PID %s ... ", pidStr)

		var stopCmd *exec.Cmd
		if runtime.GOOS == "windows" {
			stopCmd = exec.Command("taskkill", "/f", "/pid", pidStr)
		} else {
			stopCmd = exec.Command("kill", "-9", pidStr)
		}
		if err := stopCmd.Run(); err == nil {
			fmt.Println("成功")
			os.Remove(pidFile)
		} else {
			fmt.Printf("失败: %v\n", err)
		}
		return
	case "start":
		args := []string{"--cmd", "run", "--port", *port}
		binary, _ := os.Executable()
		startCmd := exec.Command(binary, args...)
		if err := startCmd.Start(); err != nil {
			fmt.Printf("后台启动失败: %v\n", err)
		} else {
			fmt.Printf("服务已在后台启动，端口: %s, PID: %d\n", *port, startCmd.Process.Pid)
		}
		return
	case "restart":
		// 简单执行 stop 和 start
		exec.Command(os.Args[0], "--cmd", "stop").Run()
		time.Sleep(1 * time.Second)
		args := []string{"--cmd", "start", "--port", *port}
		exec.Command(os.Args[0], args...).Run()
		fmt.Println("服务重启指令已发送")
		return
	}

	// 记录详细 PID 信息
	info := ProcessInfo{
		Pid:     os.Getpid(),
		Port:    ":" + *port,
		StartAt: time.Now(),
	}
	infoData, _ := json.MarshalIndent(info, "", "  ")
	os.WriteFile(pidFile, infoData, 0644)

	runServer(":" + *port)
}

func runServer(portStr string) {
	loadConfig()
	initDB()
	startSessionCleaner() // 启动 Session 过期清理后台任务

	// 初始化带连接池的全局 HTTP 客户端
	httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,              // 最大空闲连接数
			IdleConnTimeout:     90 * time.Second, // 空闲连接超时时间
			MaxIdleConnsPerHost: 20,               // 每个 Host 的最大空闲连接
		},
		Timeout: 30 * time.Second, // 代理请求硬超时
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.RemoveExtraSlash = true // 兼容 //api/ 等多斜杠请求路径
	r.Use(gin.Recovery()) // 运营商级鲁棒性：自动恢复 Panic

	// 全局 CORS 跨域配置
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", c.GetHeader("Origin"))
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, dreamId")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// 静态文件服务：将嵌入的 static 目录映射到 /static 路由
	staticFS, _ := fs.Sub(embeddedFS, "static")
	r.StaticFS("/static", http.FS(staticFS))

	// --- 1. IPTV 调度业务逻辑 ---
	// 访问示例: /iptv/cctv1.m3u8 -> 重定向到活动的源服务器
	r.GET("/iptv/:filename", func(c *gin.Context) {
		filename := c.Param("filename")
		if !strings.HasSuffix(filename, ".m3u8") {
			c.Status(404)
			return
		}

		// 0. 前置检查：数据库未连接且缓存为空
		if !dbConnected && len(cacheSources) == 0 {
			c.String(503, "调度服务暂不可用 (数据库未连接)")
			return
		}

		// 1. 从内存缓存中获取活动的直播源
		cacheLock.RLock()
		var sources []M3u8Source
		for _, s := range cacheSources {
			if s.Active {
				sources = append(sources, s)
			}
		}
		cacheLock.RUnlock()

		if len(sources) == 0 {
			c.String(503, "当前内存中没有可用的直播源调度节点")
			return
		}

		// 权重随机选择一个直播源 (实现负载均衡)
		var source M3u8Source
		totalWeight := 0
		for _, s := range sources {
			w := s.Weight
			if w <= 0 {
				w = 1
			} // 确保权重至少为 1
			totalWeight += w
		}

		if totalWeight > 0 {
			rVal := rand.Intn(totalWeight)
			currSum := 0
			for _, s := range sources {
				w := s.Weight
				if w <= 0 {
					w = 1
				}
				currSum += w
				if rVal < currSum {
					source = s
					break
				}
			}
		} else {
			source = sources[0]
		}

		name := strings.TrimSuffix(filename, ".m3u8")
		var redirectURL string
		// 根据服务器类型构建重定向地址
		if source.Type == "MistServer" {
			redirectURL = fmt.Sprintf("http://%s:%s/hls/%s/index.m3u8", source.IP, source.Port, name)
		} else if source.Type == "MediaMTX" {
			redirectURL = fmt.Sprintf("http://%s:%s/%s/index.m3u8", source.IP, source.Port, name)
		} else {
			redirectURL = fmt.Sprintf("http://%s:%s/%s.m3u8", source.IP, source.Port, name)
		}

		// 保留原有的 URL 查询参数
		if c.Request.URL.RawQuery != "" {
			redirectURL += "?" + c.Request.URL.RawQuery
		}

		log.Printf("[IPTV 调度] %s -> %s", filename, redirectURL)
		c.Redirect(http.StatusFound, redirectURL)
	})

	// 将所有 /api/* 的请求反向代理到活动的后端节点 (从内存读取)
	r.Any("/api/*any", func(c *gin.Context) {
		// 0. 前置检查
		if !dbConnected && len(cacheBackends) == 0 {
			c.String(503, "API 代理服务暂不可用 (数据库未连接)")
			return
		}

		cacheLock.RLock()
		var backend APIBackend
		found := false
		for _, b := range cacheBackends {
			if b.Active && b.State {
				backend = b
				found = true
				break
			}
		}
		cacheLock.RUnlock()

		if !found {
			c.String(503, "API 代理未开启或无可用的活动节点")
			return
		}

		targetAddr := backend.IP
		// 清理可能存在的协议头，统一强制使用 http
		targetAddr = strings.TrimPrefix(targetAddr, "http://")
		targetAddr = strings.TrimPrefix(targetAddr, "https://")
		targetAddr = strings.TrimRight(targetAddr, "/")

		proxy := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				req.URL.Scheme = "http"
				req.URL.Host = targetAddr
				req.Host = targetAddr
				// 修复双斜杠问题：清理路径中的连续斜杠
				req.URL.Path = regexp.MustCompile(`/{2,}`).ReplaceAllString(req.URL.Path, "/")
				log.Printf("[API 代理] %s -> http://%s%s", c.Request.URL.RequestURI(), targetAddr, req.URL.Path)
			},
			Transport: httpClient.Transport, // 使用全局优化的连接池
		}
		proxy.ServeHTTP(c.Writer, c.Request)
	})

	// --- 3. 管理后台管理路由 ---
	dash := r.Group("/dash")
	{
		// 0. 系统内部监控 API (供 CLI 调用)
		dash.GET("/status/sys", func(c *gin.Context) {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			c.JSON(200, gin.H{
				"alloc_mb":   m.Alloc / 1024 / 1024,
				"sys_mb":     m.Sys / 1024 / 1024,
				"goroutines": runtime.NumGoroutine(),
			})
		})

		dash.POST("/setup", func(c *gin.Context) {
			// 如果数据库已连接，修改配置必须鉴权
			if dbConnected {
				token := c.GetHeader("dreamId")
				sessLock.RLock()
				_, ok := sessions[token]
				sessLock.RUnlock()
				if !ok {
					c.JSON(401, gin.H{"status": false, "msg": "未授权：数据库在线时修改配置需登录"})
					return
				}
			}

			var nc Config
			if err := c.ShouldBindJSON(&nc); err != nil {
				c.JSON(200, gin.H{"status": false, "msg": "参数格式错误"})
				return
			}

			// 如果密码为空，则保留原有的密码
			if nc.Pass == "" {
				nc.Pass = globalConfig.Pass
			}

			// 合成并保存
			if nc.User == "" {
				nc.MongoURI = fmt.Sprintf("mongodb://%s:%s/%s", nc.Host, nc.Port, nc.DBName)
			} else {
				nc.MongoURI = fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", nc.User, nc.Pass, nc.Host, nc.Port, nc.DBName)
			}
			globalConfig = nc

			data, _ := json.MarshalIndent(globalConfig, "", "  ")
			os.WriteFile("config.ini", data, 0644)

			// 重新连接数据库
			initDB()

			// 清除所有 Session，要求用户重新登录
			sessLock.Lock()
			sessions = make(map[string]SessionInfo)
			sessLock.Unlock()

			c.JSON(200, gin.H{"status": dbConnected, "msg": "配置已保存，数据库已重新连接"})
		})

		// 1. 公开路由 (无需鉴权)
		dash.GET("/login", func(c *gin.Context) {
			renderTemplate(c, "views/login.html")
		})
		dash.GET("/setup-page", func(c *gin.Context) {
			// 数据库已正常连接 → 重定向到需要登录的系统设置页
			if dbConnected {
				c.Redirect(302, "/dash/system")
				return
			}
			// 数据库未连接 → 允许无认证访问初始化配置页
			renderTemplate(c, "views/setup.html")
		})

		// 获取当前配置信息 (脱敏版，用于 setup 页面回显)
		dash.GET("/config-info", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"host":   globalConfig.Host,
				"port":   globalConfig.Port,
				"user":   globalConfig.User,
				"dbname": globalConfig.DBName,
			})
		})

		// 登录逻辑
		dash.POST("/login", func(c *gin.Context) {
			var d struct {
				Mail     string `json:"mail"`
				Password string `json:"password"`
			}
			if err := c.ShouldBindJSON(&d); err != nil {
				c.JSON(200, gin.H{"status": false, "msg": "请求数据格式错误"})
				return
			}
			if !dbConnected {
				c.JSON(200, gin.H{"status": false, "msg": "数据库连接异常，请检查配置"})
				return
			}
			var m Member
			err := memberColl.FindOne(context.TODO(), bson.M{"mail": d.Mail}).Decode(&m)
			inputPass := passwd(d.Password)
			if err != nil || m.Password != inputPass || m.Level != 100 {
				c.JSON(200, gin.H{"status": false, "msg": "账号、密码错误或权限不足"})
				return
			}
			id := uuid.New().String()
			sessLock.Lock()
			sessions[id] = SessionInfo{
				Mail:     d.Mail,
				ExpireAt: time.Now().Add(sessionTTL),
			}
			sessLock.Unlock()
			c.SetCookie("dreamId", id, 86400, "/", "", false, false)
			c.JSON(200, gin.H{"status": true, "token": id})
		})

		// 2. 页面路由组 (未登录 → 302 跳转登录页)
		pages := dash.Group("/")
		pages.Use(pageAuthMiddleware())
		{
			pages.GET("/", func(c *gin.Context) { c.Redirect(302, "/dash/api") })
			pages.GET("/api", func(c *gin.Context) {
				renderTemplate(c, "views/api.html")
			})
			pages.GET("/m3u8", func(c *gin.Context) {
				renderTemplate(c, "views/m3u8.html")
			})
			pages.GET("/menu", func(c *gin.Context) {
				renderTemplate(c, "views/menu.html")
			})
			pages.GET("/system", func(c *gin.Context) {
				renderTemplate(c, "views/system.html")
			})
			pages.GET("/users", func(c *gin.Context) {
				renderTemplate(c, "views/users.html")
			})

			// 退出登录
			pages.GET("/logout", func(c *gin.Context) {
				token, _ := c.Cookie("dreamId")
				if token == "" {
					token = c.GetHeader("dreamId")
				}
				if token != "" {
					sessLock.Lock()
					delete(sessions, token)
					sessLock.Unlock()
				}
				// 清除 Cookie
				c.SetCookie("dreamId", "", -1, "/", "", false, false)
				c.Redirect(302, "/dash/login")
			})
		}

		// 3. API 路由组 (未登录 → 401 JSON)
		auth := dash.Group("/")
		auth.Use(authMiddleware())
		{

			// 状态接口
			auth.GET("/status", func(c *gin.Context) {
				c.JSON(200, gin.H{
					"db_connected": dbConnected,
					"config": gin.H{
						"host": globalConfig.Host, "port": globalConfig.Port,
						"user": globalConfig.User, "dbname": globalConfig.DBName,
					},
				})
			})

			// --- 数据操作 API 子组 ---
			biz := auth.Group("/biz")
			{
				// Ping 测试接口（真实系统 ping，Windows 强制 UTF-8 输出避免乱码）
				biz.GET("/ping", func(c *gin.Context) {
					target := c.Query("ip")
					if target == "" {
						c.JSON(400, gin.H{"status": false, "msg": "缺少目标 IP"})
						return
					}
					// 解析出纯 IP/域名（去掉 http(s):// 协议头和端口）
					// 预处理：去掉首尾空格
					cleanTarget := strings.TrimSpace(target)
					cleanTarget = strings.TrimPrefix(cleanTarget, "http://")
					cleanTarget = strings.TrimPrefix(cleanTarget, "https://")
					if idx := strings.Index(cleanTarget, ":"); idx != -1 {
						cleanTarget = cleanTarget[:idx]
					}
					if idx := strings.Index(cleanTarget, "/"); idx != -1 {
						cleanTarget = cleanTarget[:idx]
					}

					// 严格安全校验：只允许字母、数字、点、中划线
					re := regexp.MustCompile(`^[a-zA-Z0-9\.\-]+$`)
					if !re.MatchString(cleanTarget) {
						c.JSON(400, gin.H{"status": false, "msg": "非法目标地址"})
						return
					}

					var cmd *exec.Cmd
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()

					if runtime.GOOS == "windows" {
						// 去掉引号，确保 Windows ping 能正确识别 IP
						pingCmd := fmt.Sprintf("chcp 65001 >nul 2>&1 && ping -n 5 %s", cleanTarget)
						cmd = exec.CommandContext(ctx, "cmd", "/c", pingCmd)
					} else {
						cmd = exec.CommandContext(ctx, "ping", "-c", "5", cleanTarget)
					}
					out, err := cmd.CombinedOutput()
					if err != nil && len(out) == 0 {
						c.JSON(200, gin.H{"status": true, "output": fmt.Sprintf("Ping %s 失败: %v", cleanTarget, err)})
						return
					}
					c.JSON(200, gin.H{"status": true, "output": string(out)})
				})

				// 接口代理
				biz.GET("/backends", func(c *gin.Context) {
					cursor, _ := apiColl.Find(context.TODO(), bson.M{})
					list := []APIBackend{}
					cursor.All(context.TODO(), &list)
					c.JSON(200, list)
				})
				biz.POST("/backends", func(c *gin.Context) {
					var b APIBackend
					c.ShouldBindJSON(&b)

					// IP 唯一性校验：检查是否存在相同 IP 但不同名称的节点
					var existing APIBackend
					err := apiColl.FindOne(context.TODO(), bson.M{
						"ip":   b.IP,
						"name": bson.M{"$ne": b.Name}, // 排除自身（允许更新自己的其他属性）
					}).Decode(&existing)
					if err == nil {
						// 找到了重复 IP 的其他节点
						c.JSON(200, gin.H{"status": false, "msg": fmt.Sprintf("IP 地址 %s 已被节点 [%s] 使用，不可重复", b.IP, existing.Name)})
						return
					}

					apiColl.UpdateOne(context.TODO(), bson.M{"name": b.Name}, bson.M{"$set": b}, options.Update().SetUpsert(true))
					if b.Active {
						apiColl.UpdateMany(context.TODO(), bson.M{"name": bson.M{"$ne": b.Name}}, bson.M{"$set": bson.M{"active": false}})
					}
					loadCache()
					c.JSON(200, gin.H{"status": true})
				})

				// --- 用户管理 API ---
				biz.GET("/users", func(c *gin.Context) {
					cursor, _ := memberColl.Find(context.TODO(), bson.M{})
					var list []Member
					cursor.All(context.TODO(), &list)
					// 脱敏处理
					for i := range list {
						list[i].Password = "******"
					}
					c.JSON(200, list)
				})

				biz.POST("/user-save", func(c *gin.Context) {
					var m struct {
						Mail     string `json:"mail"`
						Password string `json:"password"`
						Name     string `json:"name"`
						Level    int    `json:"level"`
					}
					if err := c.ShouldBindJSON(&m); err != nil {
						c.JSON(200, gin.H{"status": false, "msg": "参数错误"})
						return
					}
					update := bson.M{
						"name":  m.Name,
						"level": m.Level,
					}
					if m.Password != "" && m.Password != "******" {
						update["password"] = passwd(m.Password)
					}
					_, err := memberColl.UpdateOne(context.TODO(),
						bson.M{"mail": m.Mail},
						bson.M{"$set": update},
						options.Update().SetUpsert(true),
					)
					if err != nil {
						c.JSON(200, gin.H{"status": false, "msg": "保存失败"})
					} else {
						c.JSON(200, gin.H{"status": true, "msg": "保存成功"})
					}
				})

				biz.POST("/user-del", func(c *gin.Context) {
					var d struct {
						Mail string `json:"mail"`
					}
					c.ShouldBindJSON(&d)
					count, _ := memberColl.CountDocuments(context.TODO(), bson.M{})
					if count <= 1 {
						c.JSON(200, gin.H{"status": false, "msg": "系统中必须至少保留一个账号"})
						return
					}
					_, err := memberColl.DeleteOne(context.TODO(), bson.M{"mail": d.Mail})
					if err != nil {
						c.JSON(200, gin.H{"status": false, "msg": "删除失败"})
					} else {
						c.JSON(200, gin.H{"status": true, "msg": "删除成功"})
					}
				})
				biz.DELETE("/backends", func(c *gin.Context) {
					name := c.Query("name")
					// 强制检查：活动节点不允许删除
					var check APIBackend
					apiColl.FindOne(context.TODO(), bson.M{"name": name}).Decode(&check)
					if check.Active {
						c.JSON(200, gin.H{"status": false, "msg": "该节点处于活动状态，请先激活其他节点后再删除"})
						return
					}
					apiColl.DeleteOne(context.TODO(), bson.M{"name": name})
					loadCache()
					c.JSON(200, gin.H{"status": true})
				})

				// 直播调度
				biz.GET("/sources", func(c *gin.Context) {
					cursor, _ := m3u8Coll.Find(context.TODO(), bson.M{})
					list := []M3u8Source{}
					cursor.All(context.TODO(), &list)
					c.JSON(200, list)
				})
				biz.POST("/sources", func(c *gin.Context) {
					var s M3u8Source
					c.ShouldBindJSON(&s)
					m3u8Coll.UpdateOne(context.TODO(), bson.M{"name": s.Name}, bson.M{"$set": s}, options.Update().SetUpsert(true))
					loadCache()
					c.JSON(200, gin.H{"status": true})
				})
				biz.DELETE("/sources", func(c *gin.Context) {
					name := c.Query("name")
					m3u8Coll.DeleteOne(context.TODO(), bson.M{"name": name})
					loadCache()
					c.JSON(200, gin.H{"status": true})
				})

				// 节目映射
				biz.GET("/menu", func(c *gin.Context) {
					cursor, _ := menuColl.Find(context.TODO(), bson.M{}, options.Find().SetSort(bson.M{"sort": 1}))
					var list []MenuMapping
					cursor.All(context.TODO(), &list)
					c.JSON(200, list)
				})
				biz.POST("/menu", func(c *gin.Context) {
					var list []MenuMapping
					c.ShouldBindJSON(&list)
					for _, m := range list {
						menuColl.UpdateOne(context.TODO(), bson.M{"id": m.ID}, bson.M{"$set": m}, options.Update().SetUpsert(true))
					}
					loadCache()
					c.JSON(200, gin.H{"status": true})
				})
				biz.DELETE("/menu", func(c *gin.Context) {
					id := c.Query("id")
					menuColl.DeleteOne(context.TODO(), bson.M{"id": id})
					loadCache()
					c.JSON(200, gin.H{"status": true})
				})
			}
		}
	}

	// 健康检查入口 (保持空白)
	r.GET("/", func(c *gin.Context) { c.String(200, "") })

	log.Printf("IPTV 调度系统 v%s 运行在 %s", AppVersion, portStr)
	r.Run(portStr)
}
// renderTemplate 动态渲染 HTML 模板，注入版本号、导航栏等信息
func renderTemplate(c *gin.Context, path string) {
	content, err := embeddedFS.ReadFile(path)
	if err != nil {
		log.Printf("[Error] 模板加载失败 (%s): %v", path, err)
		c.String(500, "Template Error")
		return
	}

	html := string(content)

	// 1. 生成并注入导航栏 (仅当页面包含 {{NAVBAR}} 占位符时)
	if strings.Contains(html, "{{NAVBAR}}") {
		nav := NavbarTemplate
		currentPath := c.Request.URL.Path
		// 设置 active 状态
		nav = strings.ReplaceAll(nav, "{{ACTIVE_API}}",    getBtnActive(currentPath, "/dash/api"))
		nav = strings.ReplaceAll(nav, "{{ACTIVE_M3U8}}",   getBtnActive(currentPath, "/dash/m3u8"))
		nav = strings.ReplaceAll(nav, "{{ACTIVE_MENU}}",   getBtnActive(currentPath, "/dash/menu"))
		nav = strings.ReplaceAll(nav, "{{ACTIVE_USERS}}",  getBtnActive(currentPath, "/dash/users"))
		nav = strings.ReplaceAll(nav, "{{ACTIVE_SYSTEM}}", getBtnActive(currentPath, "/dash/system"))
		html = strings.ReplaceAll(html, "{{NAVBAR}}", nav)
	}

	// 2. 注入版本号
	html = strings.ReplaceAll(html, "{{VERSION}}", AppVersion)

	c.Data(200, "text/html; charset=utf-8", []byte(html))
}

func getBtnActive(current, target string) string {
	if current == target {
		return "active"
	}
	return ""
}

