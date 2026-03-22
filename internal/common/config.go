package common

import (
	"path/filepath"

	"github.com/spf13/viper"
)

type Config struct {
	MySQL struct {
		DSN string `mapstructure:"dsn"`
	} `mapstructure:"mysql"`

	Logic struct {
		GRPCAddr string `mapstructure:"grpc_addr"`
	} `mapstructure:"logic"`

	API struct {
		HTTPAddr      string `mapstructure:"http_addr"`
		LogicGRPCAddr string `mapstructure:"logic_grpc_addr"`
	} `mapstructure:"api"`

	Redis struct {
		RedisAddr string `mapstructure:"redis_addr"`
		Password  string `mapstructure:"redis_password"`
		DB        int    `mapstructure:"redis_db"`
	} `mapstructure:"redis"`
	Session struct {
		TTLsec int64 `mapstructure:"session_ttl_seconds"`
	} `mapstructure:"session"`
	Logger struct {
		LogFilePath    string `mapstructure:"log_file_path"`
		LogFileSize    int    `mapstructure:"log_file_size"`
		LogFileBackups int    `mapstructure:"log_file_backups"`
		LogFileAge     int    `mapstructure:"log_file_age"`
		LogFileLevel   string `mapstructure:"log_file_level"`
	} `mapstructure:"logger"`
	Connect1 ConnectConfig `mapstructure:"connect1"`
	Connect2 ConnectConfig `mapstructure:"connect2"`
	Kafka    struct {
		KafkaBrokers            []string `mapstructure:"kafka_brokers"`
		SinglechatConsumerGroup string   `mapstructure:"kafka__single_consumer_group"`
		GroupchatConsumerGroup  string   `mapstructure:"kafka__group_consumer_group"`
		ChatTopic               string   `mapstructure:"kafka__chat_topic"`
		GroupChatTopic          string   `mapstructure:"kafka__group_chat_topic"`
		SmallGroupMaxMembers    int      `mapstructure:"small_group_max_members"`
		Version                 string   `mapstructure:"kafka_version"`
	} `mapstructure:"kafka"`
}
type ConnectConfig struct {
	ConnectGRPCAddr   string `mapstructure:"connect_grpc_addr"`
	ConnectHTTPAddr   string `mapstructure:"connec_http_addr"`
	ConnectServerAddr string `mapstructure:"connect_grpc_addr"`
}

func LoadConfig(path string) (*Config, error) {

	v := viper.New()

	// 配置文件路径
	v.SetConfigFile(path)

	// 根据扩展名推断格式（toml）
	v.SetConfigType(filepath.Ext(path)[1:])

	// 读取配置文件
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config

	// 绑定到 struct
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
