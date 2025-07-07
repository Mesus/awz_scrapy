package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
)

// PostgresDB 是PostgreSQL数据库连接的工具类
type PostgresDB struct {
	pool *pgxpool.Pool
}

// NewPostgresDB 创建一个新的PostgresDB实例
func NewPostgresDB() (*PostgresDB, error) {
	// 获取当前工作目录
	wd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("获取工作目录失败: %v", err)
	}
	fmt.Printf("当前工作目录: %s\n", wd)

	// 加载.env文件
	envPath := filepath.Join(wd, ".env")
	fmt.Printf("尝试加载环境变量文件: %s\n", envPath)
	err = godotenv.Load(envPath)
	if err != nil {
		return nil, fmt.Errorf("加载.env文件失败: %v", err)
	}
	fmt.Println(".env文件加载成功")

	// 构建连接字符串
	connStr := os.Getenv("PG_URL")
	if connStr == "" {
		return nil, fmt.Errorf("环境变量PG_URL未设置或为空")
	}
	fmt.Println("成功获取PostgreSQL连接字符串")

	// 创建连接池
	fmt.Println("正在连接PostgreSQL数据库...")
	// 使用带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	err = pool.Ping(pingCtx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("数据库连接测试失败: %v", err)
	}
	fmt.Println("PostgreSQL数据库连接成功")

	return &PostgresDB{pool: pool}, nil
}

// Close 关闭数据库连接
func (db *PostgresDB) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

// GetConfig 通用函数，查询configs表中指定type的记录，返回values
func (db *PostgresDB) GetConfig(configType string) (string, error) {
	var values string

	// 检查连接池是否已初始化
	if db.pool == nil {
		return "", fmt.Errorf("数据库连接池未初始化")
	}

	// 执行查询
	query := "SELECT values FROM configs WHERE type = $1"
	fmt.Printf("正在查询%s配置...\n", configType)

	// 使用带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := db.pool.QueryRow(ctx, query, configType).Scan(&values)
	if err != nil {
		return "", fmt.Errorf("查询%s配置失败: %v", configType, err)
	}

	// 检查值是否为空
	if values == "" {
		fmt.Printf("警告：%s配置值为空\n", configType)
	} else {
		fmt.Printf("成功获取%s配置，长度: %d\n", configType, len(values))
	}

	return values, nil
}

// GetClashConfig 查询configs表中type="clash"的记录，返回values
func (db *PostgresDB) GetClashConfig() (string, error) {
	return db.GetConfig("clash")
}

// GetMongoConfig 查询configs表中type="mongo"的记录，返回values
func (db *PostgresDB) GetMongoConfig() (string, error) {
	config, err := db.GetConfig("mongo")
	if err != nil {
		return "", fmt.Errorf("获取MongoDB配置失败: %v", err)
	}

	// 检查配置是否为空
	if config == "" {
		return "", fmt.Errorf("MongoDB配置为空，请检查configs表中type=mongo的记录")
	}

	// 检查配置格式
	if !strings.HasPrefix(config, "mongodb://") && !strings.HasPrefix(config, "mongodb+srv://") {
		fmt.Printf("警告：MongoDB连接字符串格式可能不正确: %s\n", config)
	}

	return config, nil
}

// GetRedisConfig 查询configs表中type="redis"的记录，返回values
func (db *PostgresDB) GetRedisConfig() (string, error) {
	return db.GetConfig("redis")
}

// GetConvConfig 查询configs表中type="conv"的记录，返回values
func (db *PostgresDB) GetConvConfig() (string, error) {
	return db.GetConfig("conv")
}

// TaskInfo 表示从keywords_scrapy_task表中查询到的任务信息
type TaskInfo struct {
	TaskID      string
	TaskType    string
	Keywords    string
	Category    string
	CountryCode string
	PageNum     int
	MinPage     int
	Zipcode     string
}

// GetTaskByID 根据任务ID查询keywords_scrapy_task表中的任务信息
func (db *PostgresDB) GetTaskByID(taskID string) (*TaskInfo, error) {
	var taskInfo TaskInfo

	// 执行查询
	query := `SELECT task_id, task_type, keywords, category, country_code, page_num, min_page, zipcode 
	         FROM keywords_scrapy_task 
	         WHERE task_id = $1`
	err := db.pool.QueryRow(context.Background(), query, taskID).Scan(
		&taskInfo.TaskID,
		&taskInfo.TaskType,
		&taskInfo.Keywords,
		&taskInfo.Category,
		&taskInfo.CountryCode,
		&taskInfo.PageNum,
		&taskInfo.MinPage,
		&taskInfo.Zipcode,
	)
	if err != nil {
		return nil, fmt.Errorf("查询任务信息失败: %v", err)
	}

	return &taskInfo, nil
}

// UpdateTaskSuccess 更新任务状态为已完成，并更新ASIN数量
func (db *PostgresDB) UpdateTaskSuccess(taskID string, asinCount int) error {
	// 执行更新
	query := `UPDATE keywords_scrapy_task 
	         SET status = '已完成', asin_num = $2, updated_at = CURRENT_TIMESTAMP 
	         WHERE task_id = $1`
	_, err := db.pool.Exec(context.Background(), query, taskID, asinCount)
	if err != nil {
		return fmt.Errorf("更新任务状态失败: %v", err)
	}

	return nil
}

// UpdateTaskFailed 更新任务状态为已失败，并记录错误信息
func (db *PostgresDB) UpdateTaskFailed(taskID string, errMsg string) error {
	// 执行更新
	query := `UPDATE keywords_scrapy_task 
	         SET status = '已失败', err_msg = $2, updated_at = CURRENT_TIMESTAMP 
	         WHERE task_id = $1`
	_, err := db.pool.Exec(context.Background(), query, taskID, errMsg)
	if err != nil {
		return fmt.Errorf("更新任务状态失败: %v", err)
	}

	return nil
}

// ExampleUsage 示例使用方法
func ExampleUsage() {
	// 创建数据库连接
	db, err := NewPostgresDB()
	if err != nil {
		fmt.Printf("创建数据库连接失败: %v\n", err)
		return
	}
	defer db.Close()

	// 获取clash配置
	clashConfig, err := db.GetClashConfig()
	if err != nil {
		fmt.Printf("获取clash配置失败: %v\n", err)
		return
	}

	fmt.Printf("Clash配置: %s\n", clashConfig)
}
