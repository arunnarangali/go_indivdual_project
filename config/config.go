package config

import "database/sql"

type DBConnector interface {
	Connect() (*sql.DB, error)
	Close() error
}

// MySQL connection configuration.
type MySQLConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

// implements the DBConnector interface for MySQL.
type MySQLConnector struct {
	config MySQLConfig
}

//Kafka connection configuration.
type KafkaConfig struct {
	Broker string
}

//implements the DBConnector interface for Kafka.
type KafkaConnector struct {
	config KafkaConfig
}

// ClickHouse connection configuration.
type ClickHouseConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

// struct that implements the DBConnector interface for ClickHouse.
type ClickHouseConnector struct {
	config ClickHouseConfig
}
