package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	logs "github.com/danbai225/go-logs"
	"github.com/go-resty/resty/v2"
	"github.com/moshaoli688/clash/config"
	C "github.com/moshaoli688/clash/constant"
	"github.com/moshaoli688/clash/hub"
	"go.uber.org/automaxprocs/maxprocs"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"awesomeProject/db"
)

func init() {
	rand.NewSource(time.Now().UnixNano())
}

// UpdateClashConfig 从数据库获取Clash配置，转换后写入clash.yaml文件
func UpdateClashConfig() error {
	// 创建数据库连接
	postgresDB, err := db.NewPostgresDB()
	if err != nil {
		return fmt.Errorf("创建数据库连接失败: %v", err)
	}
	defer postgresDB.Close()

	// 获取clash配置
	clashConfig, err := postgresDB.GetClashConfig()
	if err != nil {
		return fmt.Errorf("获取clash配置失败: %v", err)
	}

	// 获取conv配置
	convHost, err := postgresDB.GetConvConfig()
	if err != nil {
		return fmt.Errorf("获取conv配置失败: %v", err)
	}

	// URL编码配置
	encodedConfig := url.QueryEscape(clashConfig)

	// 创建resty客户端
	client := resty.New()

	// 发送请求获取转换后的配置
	resp, err := client.R().
		Get(fmt.Sprintf(convHost+":25500/sub?target=clash&url=%s", encodedConfig))

	if err != nil {
		return fmt.Errorf("请求转换服务失败: %v", err)
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("转换服务返回错误状态码: %d", resp.StatusCode())
	}

	// 获取当前工作目录
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("获取工作目录失败: %v", err)
	}

	// 构建clash.yaml文件路径
	clashYamlPath := filepath.Join(wd, "clash.yaml")

	// 写入文件
	err = os.WriteFile(clashYamlPath, resp.Body(), 0644)
	if err != nil {
		return fmt.Errorf("写入clash.yaml文件失败: %v", err)
	}

	logs.Info("成功更新clash.yaml配置文件")
	return nil
}

type Clash struct {
	configPath string
}

func New(configPath string) *Clash {
	return &Clash{configPath: configPath}
}
func (c *Clash) Start() {
	_, err := maxprocs.Set(maxprocs.Logger(func(string, ...any) {}))
	if err != nil {
		return
	}
	C.SetHomeDir(".")
	if c.configPath != "" {
		if !filepath.IsAbs(c.configPath) {
			currentDir, _ := os.Getwd()
			c.configPath = filepath.Join(currentDir, c.configPath)
		}
		C.SetConfig(c.configPath)
	} else {
		configFile := filepath.Join(C.Path.HomeDir(), C.Path.Config())
		C.SetConfig(configFile)
	}
	fmt.Println(C.Path.HomeDir())
	if err := config.Init(C.Path.HomeDir()); err != nil {
		logs.Err("Initial configuration directory error: %s", err.Error())
		return
	}
	if err := hub.Parse(hub.WithExternalController(":9091")); err != nil {
		logs.Err("Parse config error: %s", err.Error())
		return
	}
	time.Sleep(time.Second * 1)
	c.Speed()
	c.RandomSelect()
}
func (c *Clash) Speed() {
	proxies := c.Proxies()
	group := sync.WaitGroup{}
	for _, s := range proxies {
		group.Add(1)
		go func() {
			_, err := http.Get(fmt.Sprintf(`http://127.0.0.1:9091/proxies/%s/delay?timeout=5000&url=%s`, url.PathEscape(s.Name), url.QueryEscape(`https://baidu.com`)))
			if err != nil {
				return
			}
			group.Done()
		}()
	}
	group.Wait()
}
func (c *Clash) Proxies() []P {
	resp, _ := http.Get(`http://127.0.0.1:9091/proxies`)
	all, _ := io.ReadAll(resp.Body)
	m := make(map[string]interface{})
	m2 := make(map[string]P)
	err := json.Unmarshal(all, &m)
	if err != nil {
		return nil
	}
	marshal, _ := json.Marshal(m["proxies"])
	err1 := json.Unmarshal(marshal, &m2)
	if err1 != nil {
		return nil
	}
	ps := make([]P, 0)
	for k := range m2 {
		ps = append(ps, m2[k])
	}
	return ps
}
func (c *Clash) Switchover(name string) {
	jsonName := fmt.Sprintf(`{"name":"%s"}`, name)
	req, err := http.NewRequest(http.MethodPut, "http://127.0.0.1:9091/proxies/GLOBAL", bytes.NewBufferString(jsonName))
	if err != nil {
		logs.Err(err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		logs.Err(err)
		return
	}
}
func (c *Clash) EffectiveProxy() []P {
	ps := make([]P, 0)
	for _, p := range c.Proxies() {
		if len(p.History) > 0 && p.History[len(p.History)-1].Delay > 0 {
			ps = append(ps, p)
		}
	}
	return ps
}
func (c *Clash) RandomSelect() string {
	ps := c.EffectiveProxy()
	name := ps[rand.Int63n(int64(len(ps)))].Name
	c.Switchover(name)
	logs.Info("切换为", name)
	return name
}

type P struct {
	Alive   bool `json:"alive"`
	History []struct {
		Time      time.Time `json:"time"`
		Delay     int       `json:"delay"`
		MeanDelay int       `json:"meanDelay"`
	} `json:"history"`
	Name string `json:"name"`
	Type string `json:"type"`
	Udp  bool   `json:"udp"`
}
