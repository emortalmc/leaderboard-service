package config

import (
	"github.com/spf13/viper"
	"strings"
)

type Config struct {
	MongoDB *MongoDBConfig
	Redis   *RedisConfig
	Kafka   *KafkaConfig

	Port        uint16
	Development bool
}

type MongoDBConfig struct {
	URI string
}

type RedisConfig struct {
	Host string
	Port int
}

type KafkaConfig struct {
	Host string
	Port int
}

func LoadGlobalConfig() (config *Config, err error) {
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	if err = viper.ReadInConfig(); err != nil {
		return nil, err
	}

	if err = viper.UnmarshalExact(&config); err != nil {
		return nil, err
	}

	return config, nil
}
