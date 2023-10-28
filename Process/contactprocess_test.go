package Process_test

import (
	process "datastream/Process"
	"datastream/service"
	"log"
	"os"
	"testing"
)

func TestInsertContacts(t *testing.T) {
	testdatabase := "testdatabse"

	os.Setenv("MYSQL_DBNAME", testdatabase)
	dbConnector, err := service.ConfigureMySQLDB("mysql")
	if err != nil {
		log.Println("Error to get config:", err)
		t.FailNow()
	}

	// Connect to the database
	db, err := dbConnector.Connect()
	if err != nil {
		log.Println("Error connecting to the database:", err)
		t.FailNow()
	}
	defer db.Close()

	msg := []string{`avbssdda,John,johndoe@example.com,1,'{"name":"John","age":30}',
	abcdesddd,Jane,janesmith@example.com,1,'{"name":"John","age":30}'`}

	contactIDs, err := process.InsertContacts(db, msg)
	if err != nil {
		t.Fatalf("InsertContacts returned an error: %v", err)
	}

	if len(contactIDs) != 2 {
		t.Fatalf("Expected 2 contact IDs, but got %d", len(contactIDs))
	}
}

func TestInsertContactActivity(t *testing.T) {
	testdatabase := "testdatabse"

	os.Setenv("MYSQL_DBNAME", testdatabase)
	dbConnector, err := service.ConfigureMySQLDB("mysql")
	if err != nil {
		log.Println("Error to get config:", err)
		t.FailNow()
	}

	// Connect to the database
	db, err := dbConnector.Connect()
	if err != nil {
		log.Println("Error connecting to the database:", err)
		t.FailNow()
	}
	defer db.Close()

	msg := []string{`
	2cbd0fb4833c4401975d2372d368b8a2a628d680624dd712,54,4,"2023-09-04 00:00:00 +0000 UTC"
	`}

	contactIDs, err := process.InsertContactActivity(db, msg)
	if err != nil {
		t.Fatalf("InsertContacts returned an error: %v", err)
	}

	if len(contactIDs) != 1 {
		t.Fatalf("Expected 1 contact IDs, but got %d", len(contactIDs))
	}
}
