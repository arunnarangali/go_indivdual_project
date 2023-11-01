package service

import (
	"datastream/config"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestMySQLConnector_Connect(t *testing.T) {
	configData, err := config.LoadDatabaseConfig("mysql")
	if err != nil {
		t.Errorf("Expected no error for MySQL config, got: %v", err)
	}

	// Ensure the database type is MySQL.
	mysqlConfig, ok := configData.(config.MySQLConfig)

	if !ok {
		t.Errorf("expected MySQLConfig, but got %T", configData)
	}
	// Create a MySQLConnector instance.
	mysqlConnector := MySQLConnector{Config: mysqlConfig}

	// Test case 1: When the MySQLConnector is not connected (m.Db is nil).
	db, err := mysqlConnector.Connect()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if db == nil {
		t.Error("Expected a database connection, got nil")
	}

	// Test case 2: When the MySQLConnector is already connected (m.Db is not nil).
	db2, err2 := mysqlConnector.Connect()
	if err2 != nil {
		t.Errorf("Expected no error, got %v", err2)
	}
	if db2 == nil {
		t.Error("Expected a database connection, got nil")
	}

	// Test case 3: When the connection fails (for a MockMySQLConnector with Connected set to true).
	mockConnector := &MySQLConnector{Connected: true}
	_, err3 := mockConnector.Connect()
	if err3 == nil {
		t.Error("Expected a connection error, got nil")
	}
}

func TestConfigureMySQLDB(t *testing.T) {
	t.Run("Valid Configuration", func(t *testing.T) {
		validConfigMsg := "mysql"
		connector, err := ConfigureMySQLDB(validConfigMsg)
		assert.NotNil(t, connector)
		assert.Nil(t, err)
	})

	t.Run("Empty Configuration", func(t *testing.T) {
		emptyConfigMsg := ""
		connector, err := ConfigureMySQLDB(emptyConfigMsg)
		assert.Nil(t, connector)
		assert.Error(t, err)
	})

	t.Run("Invalid Configuration", func(t *testing.T) {
		invalidConfigMsg := "invalid_json"
		connector, err := ConfigureMySQLDB(invalidConfigMsg)
		assert.Nil(t, connector)
		assert.Error(t, err)
	})

	t.Run("Invalid Configuration Type", func(t *testing.T) {
		invalidTypeConfigMsg := `clickHouse`
		connector, err := ConfigureMySQLDB(invalidTypeConfigMsg)
		assert.Nil(t, connector)
		assert.Error(t, err)
	})
}

func TestInsertDataToMySql(t *testing.T) {

	configData, err := config.LoadDatabaseConfig("mysql")
	if err != nil {
		t.Errorf("Expected no error for MySQL config, got: %v", err)
	}

	mysqlConfig, ok := configData.(config.MySQLConfig)

	if !ok {
		t.Errorf("expected MySQLConfig, but got %T", configData)
	}

	mysqlConnector := MySQLConnector{Config: mysqlConfig}

	db, err := mysqlConnector.Connect()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	defer db.Close()
	// Define test data.
	tablename := "test_table"
	columnNames := []string{"id", "name", "age"}
	dataSlice := [][]interface{}{
		{1, "John", 30},
		{2, "Alice", 25},
		{3, "Bob", 35},
	}

	insertedIDs, err := InsertDataToMySql(db, tablename, columnNames, dataSlice)

	if err != nil {
		t.Errorf("Error inserting data: %v", err)
	}

	expectedLength := len(dataSlice)
	if len(insertedIDs) != expectedLength {
		t.Errorf("Expected %d last insert IDs, Got: %d", expectedLength, len(insertedIDs))
	}
}
