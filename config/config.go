package config

import (
	"datastream/logs"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type DatabaseConfig interface {
	GetConfig() map[string]string
}

type MySQLConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

type KafkaConfig struct {
	Broker string
	Topic1 string
	Topic2 string
}

type ClickHouseConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

func (m MySQLConfig) GetConfig() map[string]string {
	return map[string]string{
		"Username": m.Username,
		"Password": m.Password,
		"Hostname": m.Hostname,
		"Port":     m.Port,
		"DBName":   m.DBName,
	}
}

func (k KafkaConfig) GetConfig() map[string]string {
	return map[string]string{
		"Broker": k.Broker,
	}
}

func (c ClickHouseConfig) GetConfig() map[string]string {
	return map[string]string{
		"Username": c.Username,
		"Password": c.Password,
		"Hostname": c.Hostname,
		"Port":     c.Port,
		"DBName":   c.DBName,
	}
}

func LoadDatabaseConfig(dbType string) (DatabaseConfig, error) {
	if err := godotenv.Load("/home/arun/test 7/go_indivdual_project/.env"); err != nil {
		logs.Logger.Error("Error loading .env file:", err)
		log.Fatalf("Error loading .env file: %v", err)
		return nil, err
	}

	switch dbType {
	case "mysql":
		mysqlConfig := MySQLConfig{
			Username: os.Getenv("MYSQL_USERNAME"),
			Password: os.Getenv("MYSQL_PASSWORD"),
			Hostname: os.Getenv("MYSQL_HOSTNAME"),
			Port:     os.Getenv("MYSQL_PORT"),
			DBName:   os.Getenv("MYSQL_DBNAME"),
		}
		return mysqlConfig, nil
	case "kafka":
		kafkaConfig := KafkaConfig{
			Broker: os.Getenv("KAFKA_BROKER"),
			Topic1: os.Getenv("KAFAKA_ACTIVITY_TOPIC"),
			Topic2: os.Getenv("KAFAKA_CONTACT_TOPIC"),
		}
		return kafkaConfig, nil
	case "clickhouse":
		clickHouseConfig := ClickHouseConfig{
			Username: os.Getenv("CLICKHOUSE_USERNAME"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
			Hostname: os.Getenv("CLICKHOUSE_HOSTNAME"),
			Port:     os.Getenv("CLICKHOUSE_PORT"),
			DBName:   os.Getenv("CLICKHOUSE_DBNAME"),
		}
		return clickHouseConfig, nil
	default:
		logs.Logger.Warning("unsupported DB_TYPE:")
		return nil, fmt.Errorf("unsupported DB_TYPE: %s", dbType)
	}
}
