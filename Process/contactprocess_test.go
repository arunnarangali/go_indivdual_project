package Process_test

import (
	"datastream/Process"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertmsgIntegration(t *testing.T) {
	// Set up a test MySQL database for integration testing.

	t.Run("Valid Data Insertion", func(t *testing.T) {

		testdatabase := "testdatabse"

		os.Setenv("MYSQL_DBNAME", testdatabase)

		// Insert a test topic into the database.
		topic := "ActivityContactTopicNew60"
		err := Process.Insertmsg([]string{`4c88502a43d9444690a8dafdba9c8eeaf7d408378919422f,83,4,"2023-11-02 00:00:00 +0000 UTC"`}, topic)

		assert.Nil(t, err)

	})

	t.Run("Invalid Data Insertion (Topic Does Not Exist)", func(t *testing.T) {
		// Set up a test MySQL database with an empty topics table.

		msg := []string{"message1", "message2"}
		topic := "invalid_topic"
		err := Process.Insertmsg(msg, topic)
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
		err := Process.Insertmsg(msg, topic)

		// Verify that the error indicates the topic doesn't exist.
		expectedErrMsg := "Unknown database 'testsdatabse'"
		assert.Contains(t, err.Error(), expectedErrMsg)
		assert.Error(t, err)

	})

}

// func TestInsertContacts(t *testing.T) {
// 	testdatabase := "testdatabse"

// 	os.Setenv("MYSQL_DBNAME", testdatabase)
// 	dbConnector, err := service.ConfigureMySQLDB("mysql")
// 	if err != nil {
// 		log.Println("Error to get config:", err)
// 		t.FailNow()
// 	}

// 	// Connect to the database
// 	db, err := dbConnector.Connect()
// 	if err != nil {
// 		log.Println("Error connecting to the database:", err)
// 		t.FailNow()
// 	}
// 	defer db.Close()

// 	msg := []string{`avbssdda,John,johndoe@example.com,1,'{"name":"John","age":30}',
// 	abcdesddd,Jane,janesmith@example.com,1,'{"name":"John","age":30}'`}

// 	contactIDs, err := process.InsertContacts(db, msg)
// 	if err != nil {
// 		t.Fatalf("InsertContacts returned an error: %v", err)
// 	}

// 	if len(contactIDs) != 2 {
// 		t.Fatalf("Expected 2 contact IDs, but got %d", len(contactIDs))
// 	}
// }

// func TestInsertContactActivity(t *testing.T) {
// 	testdatabase := "testdatabse"

// 	os.Setenv("MYSQL_DBNAME", testdatabase)
// 	dbConnector, err := service.ConfigureMySQLDB("mysql")
// 	if err != nil {
// 		log.Println("Error to get config:", err)
// 		t.FailNow()
// 	}

// 	// Connect to the database
// 	db, err := dbConnector.Connect()
// 	if err != nil {
// 		log.Println("Error connecting to the database:", err)
// 		t.FailNow()
// 	}
// 	defer db.Close()

// 	msg := []string{`
// 	2cbd0fb4833c4401975d2372d368b8a2a628d680624dd712,54,4,"2023-09-04 00:00:00 +0000 UTC"
// 	`}

// 	contactIDs, err := process.InsertContactActivity(db, msg)
// 	if err != nil {
// 		t.Fatalf("InsertContacts returned an error: %v", err)
// 	}

// 	if len(contactIDs) != 1 {
// 		t.Fatalf("Expected 1 contact IDs, but got %d", len(contactIDs))
// 	}
// }
