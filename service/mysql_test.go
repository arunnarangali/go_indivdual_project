package service

import (
	"database/sql"
	"datastream/config"
	"errors"
	"testing"
)

// Create a mock for MySQLConnector that implements the Connect method.
type MockMySQLConnector struct {
	Db        *sql.DB
	Connected bool
}

func (m *MockMySQLConnector) Connect() (*sql.DB, error) {
	if m.Db != nil {
		return m.Db, nil
	}

	if m.Connected {
		return nil, errors.New("connection error")
	}

	m.Db = &sql.DB{} // Mocked database connection
	return m.Db, nil
}

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

// func TestInsertmsg(t *testing.T) {
// 	// Create a mock MySQLConnector for testing.
// 	mockConnector := &MockMySQLConnector{}

// 	// Original functions in your code
// var ConfigureMySQLDB = func() (*MySQLConnector, error) { }
// var handleTopic = func(db *sql.DB, msg []string, topic string) error { }

// // Wrapper functions for testing
// func configureMySQLDB()(*MySQLConnector, error) {
//     return ConfigureMySQLDB()
// }

// func setHandleTopic(mockFunc func(db *sql.DB, msg []string, topic string) error) {
//     handleTopic = mockFunc
// }

// 	// Test the Insertmsg function.
// 	err := Insertmsg([]string{}, "kafka_contact_topic")
// 	if err != nil {
// 		t.Errorf("Expected no error, but got: %v", err)
// 	}

// 	err = Insertmsg([]string{}, "kafka_activity_topic")
// 	if err != nil {
// 		t.Errorf("Expected no error, but got: %v", err)
// 	}

// 	err = Insertmsg([]string{}, "unknown_topic")
// 	if err == nil {
// 		t.Errorf("Expected an error, but got nil")
// 	}
// }

// func TestInsertContacts(t *testing.T) {
//     // Create a mock database connection for testing.
//     mockDB := &sql.DB{}

//     // Test the InsertContacts function.
//     contactStatus := types.ContactStatus{
//         Contact: types.Contacts{
//             ID:      "abcdef",
//             Name:    "John Doe",
//             Email:   "john@example.com",
//             Details: "Some details",
//         },
//         Status: 1,
//     }
//     contactStatuses := []types.ContactStatus{contactStatus}

//     // Replace the original ConfigureMySQLDB with the wrapper function
//     originalConfigureMySQLDB := ConfigureMySQLDB
//     ConfigureMySQLDB = configureMySQLDB
//     defer func() { ConfigureMySQLDB = originalConfigureMySQLDB }()

//     contactIDs, err := InsertContacts(mockDB, contactStatuses)
//     if err != nil {
//         t.Errorf("Expected no error, but got: %v", err)
//     }
//     if len(contactIDs) != 1 {
//         t.Errorf("Expected 1 contact ID, but got %d", len(contactIDs))
//     }
// }

// func TestInsertContactActivity(t *testing.T) {
// 	// Create a mock database connection for testing.
// 	mockDB := &sql.DB{}

// 	layout := "2006-01-02 15:04:05"

// 	// Parse the date and time string to create a time.Time object
// 	timeStr := "2023-10-24 14:30:45"
// 	activityDate, err := time.Parse(layout, timeStr)

// 	if err != nil {
// 		t.Errorf("Error parsing time: %v", err)
// 		return
// 	}

// 	// Test the InsertContactActivity function.
// 	contactActivity := types.ContactActivity{
// 		ContactID:    "abcdef",
// 		CampaignID:   2,
// 		ActivityType: 1, // This should match the data type in your function.
// 		ActivityDate: activityDate,
// 	}
// 	contactActivities := []types.ContactActivity{contactActivity}

// 	contactIDs, err := InsertContactActivity(mockDB, contactActivities)
// 	if err != nil {
// 		t.Errorf("Expected no error, but got: %v", err)
// 	}
// 	if len(contactIDs) != 1 {
// 		t.Errorf("Expected 1 contact ID, but got %d", len(contactIDs))
// 	}
// }
