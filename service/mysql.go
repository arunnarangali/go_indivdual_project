package service

import (
	"database/sql"
	"datastream/config"
	"datastream/types"

	"log"
	"os"

	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

// DBConnector is an interface for database connectors.
type DBConnector interface {
	Connect() (*sql.DB, error)
	Close() error
}

// MySQLConnector implements the DBConnector interface for MySQL.
type MySQLConnector struct {
	Config config.MySQLConfig // Use the MySQLConfig from the config package
	Db     *sql.DB
}

// // ClickHouseConnector implements the DBConnector interface for ClickHouse.
// type ClickHouseConnector struct {
// 	config config.ClickHouseConfig // Use the ClickHouseConfig from the config package
// 	db     *sql.DB
// }

// Implement the Connect method for MySQLConnector.
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

// Implement the Close method for MySQLConnector.
func (m *MySQLConnector) Close() error {
	if m.Db != nil {
		err := m.Db.Close()
		m.Db = nil
		return err
	}
	return nil
}

func ConfigureMySQLDB() (*MySQLConnector, error) {

	configData, err := config.LoadDatabaseConfig("mysql")
	if err != nil {
		return nil, fmt.Errorf("failed to load database config: %v", err)
	}
	// Ensure the database type is MySQL.
	mysqlConfig, ok := configData.(config.MySQLConfig)
	if !ok {
		return nil, fmt.Errorf("expected MySQLConfig, but got %T", configData)
	}
	// Create a MySQLConnector instance.
	mysqlConnector := MySQLConnector{Config: mysqlConfig}

	return &mysqlConnector, nil
}

func Getmsg(msg []string, topic string) error {

	dbConnector, err := ConfigureMySQLDB()
	if err != nil {
		return fmt.Errorf("error to get config: %v", err)
	}
	// Connect to the database
	db, err := dbConnector.Connect()
	if err != nil {
		return fmt.Errorf("error connecting to the database: %v", err)
	}
	defer db.Close()
	err = handleTopic(db, msg, topic)
	if err != nil {
		return fmt.Errorf("error in handle topic: %v", err)
	}
	return nil
}

func handleTopic(db *sql.DB, msg []string, topic string) error {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
		return err
	}

	if topic == os.Getenv("KAFAKA_CONTACT_TOPIC") {
		// Call InsertContact to insert the contacts
		Contactid, err := InsertContact(db, msg)
		if err != nil {
			return fmt.Errorf("error inserting contact: %v", err)
		}
		fmt.Println(Contactid)
	} else if topic == os.Getenv("KAFAKA_ACTIVITY_TOPIC") {
		activityid, err := InsertContactActivity(db, msg)
		if err != nil {
			return fmt.Errorf("error inserting contact activity: %v", err)
		}
		fmt.Println(activityid)
	}

	return nil
}

func InsertContact(db *sql.DB, msg []string) ([]int64, error) {
	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("error starting a transaction: %v", err)
	}
	defer tx.Rollback() // Rollback the transaction if there is an error

	contactIDs := []int64{}

	contactStatuses, err := types.Extractmsgcontacts(msg)
	if err != nil {
		return nil, fmt.Errorf("error extracting contact statuses: %v", err)
	}

	// Perform the insert operation with the transaction
	for _, contactStatus := range contactStatuses {
		lastInsertID, err := InsertSingleContactWithTx(tx,
			contactStatus.Contact.Name, contactStatus.Contact.Email, contactStatus.Contact.Details,
			contactStatus.Status)
		if err != nil {
			return nil, fmt.Errorf("error inserting contact: %v", err)
		}
		contactIDs = append(contactIDs, lastInsertID)
	}

	// Commit the transaction if all inserts were successful
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing transaction: %v", err)
	}

	return contactIDs, nil
}

func InsertSingleContactWithTx(tx *sql.Tx, name, email, details string, status int) (int64, error) {
	// Insert a single contact using the provided transaction
	// Replace this with your actual insert query
	result, err := tx.Exec("INSERT INTO contacts (name, email, details, status) VALUES (?, ?, ?, ?)",
		name, email, details, status)
	if err != nil {
		return 0, err
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return lastInsertID, nil
}

func InsertContactActivity(db *sql.DB, msg []string) ([]int64, error) {
	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("error starting a transaction: %v", err)
	}
	defer tx.Rollback() // Rollback the transaction if there is an error

	contactIDs := []int64{}

	contactActivity, err := types.ExtractmsgActivity(msg)
	if err != nil {
		return nil, fmt.Errorf("error extracting contact statuses: %v", err)
	}

	// Perform the insert operation with the transaction
	for _, activity := range contactActivity {
		lastInsertID, err := InsertSingleContactActivityWithTx(tx,
			activity.ContactID, activity.CampaignID, activity.ActivityType, activity.ActivityDate)
		if err != nil {
			return nil, fmt.Errorf("error inserting contact: %v", err)
		}
		contactIDs = append(contactIDs, lastInsertID)
	}

	// Commit the transaction if all inserts were successful
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing transaction: %v", err)
	}

	return contactIDs, nil
}

func InsertSingleContactActivityWithTx(tx *sql.Tx, contactsid, campaignid, activitytype int, actiivtydate string) (int64, error) {
	// Insert a single contact using the provided transaction
	// Replace this with your actual insert query
	result, err := tx.Exec("INSERT INTO contact_activity (ContactsID,CampaignID,ActivityType,ActivityDate)VALUES (?, ?, ?, ?)",
		contactsid, campaignid, activitytype, actiivtydate)
	if err != nil {
		return 0, err
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return lastInsertID, nil
}
