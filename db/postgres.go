package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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

	// 加载.env文件
	envPath := filepath.Join(wd, ".env")
	err = godotenv.Load(envPath)
	if err != nil {
		return nil, fmt.Errorf("加载.env文件失败: %v", err)
	}

	// 构建连接字符串
	connStr := os.Getenv("PG_URL")

	// 创建连接池
	pool, err := pgxpool.Connect(context.Background(), connStr)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

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

	// 执行查询
	query := "SELECT values FROM configs WHERE type = $1"
	err := db.pool.QueryRow(context.Background(), query, configType).Scan(&values)
	if err != nil {
		return "", fmt.Errorf("查询%s配置失败: %v", configType, err)
	}

	return values, nil
}

// GetClashConfig 查询configs表中type="clash"的记录，返回values
func (db *PostgresDB) GetClashConfig() (string, error) {
	return db.GetConfig("clash")
}

// GetMongoConfig 查询configs表中type="mongo"的记录，返回values
func (db *PostgresDB) GetMongoConfig() (string, error) {
	return db.GetConfig("mongo")
}

// GetRedisConfig 查询configs表中type="redis"的记录，返回values
func (db *PostgresDB) GetRedisConfig() (string, error) {
	return db.GetConfig("redis")
}

// GetConvConfig 查询configs表中type="conv"的记录，返回values
func (db *PostgresDB) GetConvConfig() (string, error) {
	return db.GetConfig("conv")
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
