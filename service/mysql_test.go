package service

import (
	"datastream/config"
	"os"
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

func TestInsertmsgIntegration(t *testing.T) {
	// Set up a test MySQL database for integration testing.

	t.Run("Valid Data Insertion", func(t *testing.T) {

		testdatabase := "testdatabse"

		os.Setenv("MYSQL_DBNAME", testdatabase)

		// Insert a test topic into the database.
		topic := "ActivityContactTopicNew53"
		err := Insertmsg([]string{`4c88502a43d9444690a8dafdba9c8eeaf7d408378919422f,83,4,"2023-11-02 00:00:00 +0000 UTC"`}, topic)

		assert.Nil(t, err)

	})

	t.Run("Invalid Data Insertion (Topic Does Not Exist)", func(t *testing.T) {
		// Set up a test MySQL database with an empty topics table.

		msg := []string{"message1", "message2"}
		topic := "invalid_topic"
		err := Insertmsg(msg, topic)
		assert.Error(t, err)

		// Verify that the error indicates the topic doesn't exist.
		expectedErrMsg := "invalid Topic"
		assert.Contains(t, err.Error(), expectedErrMsg)

	})

	t.Run("Database Connection Error", func(t *testing.T) {
		testdatabase := "testsdatabse"

		os.Setenv("MYSQL_DBNAME", testdatabase)

		msg := []string{"message1", "message2"}
		topic := "existing_topic"
		err := Insertmsg(msg, topic)

		// Verify that the error indicates the topic doesn't exist.
		expectedErrMsg := "Unknown database 'testsdatabse'"
		assert.Contains(t, err.Error(), expectedErrMsg)
		assert.Error(t, err)

	})

}
