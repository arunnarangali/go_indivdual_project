package config

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

// DatabaseConfig is an interface for database configurations.
type DatabaseConfig interface {
	GetConfig() map[string]string
}

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
	Config MySQLConfig
	Db     *sql.DB
}

// Kafka connection configuration.
type KafkaConfig struct {
	Broker string
	Topic1 string
	Topic2 string
}

// KafkaConnector implements the DBConnector interface for Kafka.
type KafkaConnector struct {
	config   KafkaConfig
	Producer sarama.SyncProducer
	Consumer sarama.Consumer
}

// ClickHouseConfig represents the configuration for ClickHouse.
type ClickHouseConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

// ClickHouseConnector implements the DBConnector interface for ClickHouse.
type ClickHouseConnector struct {
	config ClickHouseConfig
	db     *sql.DB
}

// Implement the DatabaseConfig interface for MySQLConfig.
func (m MySQLConfig) GetConfig() map[string]string {
	return map[string]string{
		"Username": m.Username,
		"Password": m.Password,
		"Hostname": m.Hostname,
		"Port":     m.Port,
		"DBName":   m.DBName,
	}
}

// Connect establishes a connection to the MySQL database.
func (m *MySQLConnector) Connect() (*sql.DB, error) {
	if m.Db != nil {
		return m.Db, nil
	}

	// Create a MySQL database connection.
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		m.Config.Username,
		m.Config.Password,
		m.Config.Hostname,
		m.Config.Port,
		m.Config.DBName)

	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}

	// Test the connection.
	if err := db.Ping(); err != nil {
		return nil, err
	}

	m.Db = db
	return db, nil
}

func (m *MySQLConnector) Close() error {
	if m.Db != nil {
		err := m.Db.Close()
		m.Db = nil
		return err
	}
	return nil
}

// Create a new KafkaConnector instance with the provided KafkaConfig.
func NewKafkaConnector(config KafkaConfig) (*KafkaConnector, error) {

	// Initialize a Kafka producer.
	producer, err := sarama.NewSyncProducer([]string{config.Broker}, nil)
	if err != nil {
		return nil, err
	}

	// Initialize a Kafka consumer.
	consumer, err := sarama.NewConsumer([]string{config.Broker}, nil)
	if err != nil {
		return nil, err
	}

	return &KafkaConnector{
		config:   config,
		Producer: producer,
		Consumer: consumer,
	}, nil
}

// Close the Kafka connections when done.
func (kc *KafkaConnector) Close() {
	if kc.Producer != nil {
		kc.Producer.Close()
	}
	if kc.Consumer != nil {
		kc.Consumer.Close()
	}
}

// Implement the DatabaseConfig interface for KafkaConfig.
func (k KafkaConfig) GetConfig() map[string]string {
	return map[string]string{
		"Broker": k.Broker,
	}
}

// Implement the DatabaseConfig interface for ClickHouseConfig.
func (c ClickHouseConfig) GetConfig() map[string]string {
	return map[string]string{
		"Username": c.Username,
		"Password": c.Password,
		"Hostname": c.Hostname,
		"Port":     c.Port,
		"DBName":   c.DBName,
	}
}

// Connect establishes a connection to the ClickHouse database.
func (c *ClickHouseConnector) Connect() (*sql.DB, error) {
	if c.db != nil {
		return c.db, nil
	}

	// Create a ClickHouse database connection.
	dataSourceName := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s&database=%s",
		c.config.Hostname,
		c.config.Port,
		c.config.Username,
		c.config.Password,
		c.config.DBName)

	db, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {
		return nil, err
	}

	// Test the connection.
	if err := db.Ping(); err != nil {
		return nil, err
	}

	c.db = db
	return db, nil
}

// Close closes the ClickHouse database connection.
func (c *ClickHouseConnector) Close() error {
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}

func LoadDatabaseConfig(dbType string) (DatabaseConfig, error) {
	if err := godotenv.Load(); err != nil {
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
		return nil, fmt.Errorf("unsupported DB_TYPE: %s", dbType)
	}
}
