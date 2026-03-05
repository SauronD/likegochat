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

	JWT struct {
		Issuer        string `mapstructure:"issuer"`
		Audience      string `mapstructure:"audience"`
		Secret        string `mapstructure:"secret"`
		AccessTTLSec  int64  `mapstructure:"access_ttl_seconds"`
		SessionTTLSec int64  `mapstructure:"session_ttl_seconds"`
	} `mapstructure:"jwt"`
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
