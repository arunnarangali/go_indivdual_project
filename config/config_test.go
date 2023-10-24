package config

import (
	"testing"
)

func TestLoadDatabaseConfig(t *testing.T) {
	// Test loading MySQL config
	mysqlConfig, err := LoadDatabaseConfig("mysql")
	if err != nil {
		t.Errorf("Expected no error for MySQL config, got: %v", err)
	}
	if _, ok := mysqlConfig.(MySQLConfig); !ok {
		t.Errorf("Expected MySQLConfig, got %T", mysqlConfig)
	}

	// Test loading Kafka config
	kafkaConfig, err := LoadDatabaseConfig("kafka")
	if err != nil {
		t.Errorf("Expected no error for Kafka config, got: %v", err)
	}
	if _, ok := kafkaConfig.(KafkaConfig); !ok {
		t.Errorf("Expected KafkaConfig, got %T", kafkaConfig)
	}

	// Test loading ClickHouse config
	clickHouseConfig, err := LoadDatabaseConfig("clickhouse")
	if err != nil {
		t.Errorf("Expected no error for ClickHouse config, got: %v", err)
	}
	if _, ok := clickHouseConfig.(ClickHouseConfig); !ok {
		t.Errorf("Expected ClickHouseConfig, got %T", clickHouseConfig)
	}

	// Test loading an unsupported config
	unsupportedConfig, err := LoadDatabaseConfig("unknown")
	if err == nil {
		t.Errorf("Expected an error for an unsupported config, got nil")
	}
	if unsupportedConfig != nil {
		t.Errorf("Expected nil for an unsupported config, got %v", unsupportedConfig)
	}
}
