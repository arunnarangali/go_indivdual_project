package Process_test

import (
	"datastream/Process"
	"datastream/logs"
	"datastream/service"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
)

func TestExtractContactsFromMessage(t *testing.T) {
	tests := []struct {
		name        string
		msg         []string
		expectError bool
	}{
		{
			name: "Successful extraction and insertion",
			msg: []string{`avbssdda,John,johndoe@example.com,1,'{"name":"John","age":30}',
			abcdesddd,Jane,janesmith@example.com,1,'{"name":"John","age":30}'`},
			expectError: false,
		},
		{
			name:        "Empty message",
			msg:         []string{},
			expectError: true,
		},
	}

	dbConnector, err := service.ConfigureMySQLDB("mysql")
	if err != nil {
		logs.Logger.Error("error to get config", err)
	}

	// Connect to the database
	db, err := dbConnector.Connect()
	if err != nil {
		logs.Logger.Error("error connecting to the database:", err)
	}

	defer db.Close()

	// Run subtests for each test case
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Process.ExtractContactsFromMessage(db, test.msg)
			if test.expectError && err == nil {
				t.Errorf("Test %q: Expected an error, but got nil", test.name)
			}
			if !test.expectError && err != nil {
				t.Errorf("Test %q: Expected no error, but got an error: %v", test.name, err)

			}
		})
	}
}

func TestExtractActivityFromMessage(t *testing.T) {
	tests := []struct {
		name        string
		msg         []string
		expectError bool
	}{
		{
			name:        "Successful extraction and insertion",
			msg:         []string{`4c88502a43d9444690a8dafdba9c8eeaf7d408378919422f,83,4,"2023-11-02 00:00:00 +0000 UTC"`},
			expectError: false,
		},
		{
			name:        "Empty message",
			msg:         []string{},
			expectError: true,
		},
	}

	dbConnector, err := service.ConfigureMySQLDB("mysql")
	if err != nil {
		logs.Logger.Error("error to get config", err)
	}

	// Connect to the database
	db, err := dbConnector.Connect()
	if err != nil {
		logs.Logger.Error("error connecting to the database:", err)
	}

	defer db.Close()

	// Run subtests for each test case
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Process.ExtractActivityFromMessage(db, test.msg)
			if test.expectError && err == nil {
				t.Errorf("Test %q: Expected an error, but got nil", test.name)
			}
			if !test.expectError && err != nil {
				t.Errorf("Test %q: Expected no error, but got an error: %v", test.name, err)

			}
		})
	}
}

func TestInsertmsgIntegration(t *testing.T) {

	t.Run("Valid Data Insertion", func(t *testing.T) {

		// Insert a test topic into the database.
		topic := "ActivityContactTopicNew60"
		msg := []string{`4c88502a43d9444690a8dafdba9c8eeaf7d408378919422f,83,4,"2023-11-02 00:00:00 +0000 UTC"`}
		err := Process.Insertmsg(msg, topic)

		assert.Nil(t, err)

	})

	t.Run("Invalid Data Insertion (Topic Does Not Exist)", func(t *testing.T) {

		msg := []string{"message1", "message2"}
		topic := "invalid_topic"
		err := Process.Insertmsg(msg, topic)
		assert.Error(t, err)

		// Verify that the error indicates the topic doesn't exist.
		expectedErrMsg := "invalid Topic"
		assert.Contains(t, err.Error(), expectedErrMsg)

	})

	t.Run("Database Connection Error", func(t *testing.T) {
		// Set up a test MySQL database for integration testing.
		if err := godotenv.Load("/home/arun/test 7/go_indivdual_project/.env"); err != nil {
			logs.Logger.Error("Error loading .env file:", err)
		}
		testdatabase := "wrongdatabase"

		os.Setenv("MYSQL_DBNAME", testdatabase)

		msg := []string{"message1", "message2"}
		topic := "existing_topic"
		err := Process.Insertmsg(msg, topic)

		// Verify that the error indicates the topic doesn't exist.
		expectedErrMsg := "Unknown database 'wrongdatabase'"
		assert.Contains(t, err.Error(), expectedErrMsg)
		assert.Error(t, err)

	})

}
