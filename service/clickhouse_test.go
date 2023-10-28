package service

import (
	"datastream/config"
	"datastream/types"
	"reflect"
	"testing"
)

func TestClickHouseConnector_Connect(t *testing.T) {
	t.Run("Successful Connection", func(t *testing.T) {

		validConfig := config.ClickHouseConfig{
			Hostname: "localhost",
			Port:     "9000",
			DBName:   "arun_campaign",
			Username: "default",
			Password: "root",
		}

		connector := &ClickHouseConnector{config: validConfig}

		db, err := connector.Connect()

		if err != nil {
			t.Errorf("Expected no error during connection, got: %v", err)
		} else {

			if db == nil {
				t.Error("Expected a valid database connection, got nil")
			}

			closeErr := db.Close()
			if closeErr != nil {
				t.Errorf("Error when closing the database connection: %v", closeErr)
			}
		}
	})
	t.Run("Reconnecting", func(t *testing.T) {

		validConfig := config.ClickHouseConfig{
			Hostname: "localhost",
			Port:     "9000",
			DBName:   "arun_campaign",
			Username: "default",
			Password: "root",
		}

		connector := &ClickHouseConnector{config: validConfig}

		// Connect for the first time
		db1, err1 := connector.Connect()
		defer func() {
			if closeErr := db1.Close(); closeErr != nil {
				t.Errorf("Error when closing the database connection (db1): %v", closeErr)
			}
		}()

		// Connect again
		db2, err2 := connector.Connect()
		defer func() {
			if closeErr := db2.Close(); closeErr != nil {
				t.Errorf("Error when closing the database connection (db2): %v", closeErr)
			}
		}()

		// Check for errors during connection
		if err1 != nil || err2 != nil {
			t.Errorf("Expected no error during connection, got: err1: %v, err2: %v", err1, err2)
		}

		// Check if both database connections are the same
		if db1 != db2 {
			t.Error("Expected the same database connection, got different connections")
		}
	})

	t.Run("Invalid Configuration", func(t *testing.T) {
		// Create a ClickHouseConfig with invalid connection details (e.g., missing hostname)
		invalidConfig := config.ClickHouseConfig{}

		connector := &ClickHouseConnector{config: invalidConfig}

		db, err := connector.Connect()

		// Check for an error indicating a failed connection
		if err == nil {
			t.Error("Expected an error, but got nil")
		}

		// Check if the database connection is nil
		if db != nil {
			t.Error("Expected a nil database connection, got a non-nil connection")
		}
	})
}

func TestClickHouseConnector_Close(t *testing.T) {
	t.Run("Closing an Open Connection", func(t *testing.T) {
		// Create a ClickHouseConfig with valid connection details
		validConfig := config.ClickHouseConfig{
			Hostname: "localhost",
			Port:     "9000",
			DBName:   "arun_campaign",
			Username: "default",
			Password: "root",
		}

		connector := &ClickHouseConnector{config: validConfig}

		// Connect to establish a connection
		connector.Connect()

		// Close the connection using the Close method
		err := connector.Close()

		// Check for errors
		if err != nil {
			t.Errorf("Expected no error when closing, got: %v", err)
		}

		if connector.db != nil {
			t.Error("Expected the database connection to be nil after closing, but it is not nil")
		}
	})

	t.Run("Closing Already Closed Connection", func(t *testing.T) {
		// Create a ClickHouseConfig with valid connection details
		validConfig := config.ClickHouseConfig{
			Hostname: "localhost",
			Port:     "9000",
			DBName:   "arun_campaign",
			Username: "default",
			Password: "root",
		}

		connector := &ClickHouseConnector{config: validConfig}

		connector.db = nil

		err := connector.Close()

		// Check for errors (should not error)
		if err != nil {
			t.Errorf("Expected no error when closing an already closed connection, got: %v", err)
		}
	})
}

func TestConfigureClickHouseDB(t *testing.T) {
	t.Run("Successful Configuration Load", func(t *testing.T) {
		// Set up your test environment for a successful configuration load.
		connector, err := ConfigureClickHouseDB("clickhouse")

		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}

		if connector == nil {
			t.Error("Expected a non-nil connector, but got nil")
		}
	})

	t.Run("Failed Configuration Load", func(t *testing.T) {
		// Set up your test environment for a failed configuration load.
		connector, err := ConfigureClickHouseDB("wrongConfigMsg")

		if err == nil {
			t.Error("Expected an error, but got nil")
		}

		if connector != nil {
			t.Error("Expected a nil connector, but got non-nil")
		}
	})

	t.Run("Non-ClickHouse Configuration Data", func(t *testing.T) {
		// Set up your test environment to simulate loading a different type of configuration data.
		connector, err := ConfigureClickHouseDB("kafka")

		if err == nil {
			t.Error("Expected an error, but got nil")
		}

		if connector != nil {
			t.Error("Expected a nil connector, but got non-nil")
		}
	})

	t.Run("Missing Configuration Data", func(t *testing.T) {
		// Set up your test environment to simulate missing configuration data.
		connector, err := ConfigureClickHouseDB("")

		if err == nil {
			t.Error("Expected an error, but got nil")
		}

		if connector != nil {
			t.Error("Expected a nil connector, but got non-nil")
		}
	})
}

func TestQueryTopContactActivity(t *testing.T) {
	t.Run("Successful Query Execution", func(t *testing.T) {

		// Ensure it returns expected results.

		query := `
		SELECT ContactsID, clicked
		FROM arun_campaign.contact_activity_summary_mv_last_three_month_summery  FINAL
		ORDER BY clicked DESC
		LIMIT 5
	`

		results, err := QueryTopContactActivity(query)
		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}
		expectedResults := []types.QueryOutput{
			{ContactID: "164d10deafc8413484581ba9ea050b3cbec846af49803216", Click: 63},
			{ContactID: "2f87057dfcd746e0bf307c8658eef8757e6389e4ebdf237d", Click: 63},
			{ContactID: "486c618b1bdf454eb72b45da19c48ffa7f68635707e87e41", Click: 62},
			{ContactID: "e0fe1144db3e4bcda706cc3b327ed606928d0037b93f4a2d", Click: 62},
			{ContactID: "8e45efa839434302ab39ec90cb1104939a42ceb043a860d6", Click: 62},
		}
		if !reflect.DeepEqual(results, expectedResults) {
			t.Errorf("Results do not match the expected results")
		}
	})

	t.Run("Failed Query Execution", func(t *testing.T) {
		// Set up a test ClickHouse configuration and database with a query

		query := "SELECT * FROM NonExistentTable"
		results, err := QueryTopContactActivity(query)
		if err == nil {
			t.Error("Expected an error, but got nil")
		}
		if results != nil {
			t.Error("Expected results to be nil, but got non-nil")
		}
	})

	t.Run("No Rows Returned", func(t *testing.T) {
		// Set up a test ClickHouse configuration and database with a query
		// that returns no rows
		query := `SELECT ContactsID, clicked
		FROM arun_campaign.contact_activity_summary_mv_last_three_month_summery  FINAL
		ORDER BY clicked DESC
		LIMIT 0`
		results, err := QueryTopContactActivity(query)
		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}
		if len(results) > 0 {
			t.Error("Expected no results, but got some")
		}
	})
}
