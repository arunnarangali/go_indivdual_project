package service

import (
	"database/sql"
	"datastream/config"
	"datastream/logs"
	"datastream/types"
	"strconv"

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
		logs.Logger.Error("error:", err)
		return nil, err
	}

	// Test the connection.
	if err := db.Ping(); err != nil {
		logs.Logger.Error("error:", err)
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
		logs.Logger.Error("error:", err)
		return err
	}
	return nil
}

func ConfigureMySQLDB() (*MySQLConnector, error) {

	configData, err := config.LoadDatabaseConfig("mysql")
	if err != nil {
		logs.Logger.Error("error:", err)
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

func Insertmsg(msg []string, topic string) error {

	dbConnector, err := ConfigureMySQLDB()
	if err != nil {
		logs.Logger.Error("error to get config", err)
		return fmt.Errorf("error to get config: %v", err)
	}
	// Connect to the database
	db, err := dbConnector.Connect()
	if err != nil {
		logs.Logger.Error("error connecting to the database:", err)
		return fmt.Errorf("error connecting to the database: %v", err)
	}
	defer db.Close()
	err = handleTopic(db, msg, topic)
	if err != nil {
		logs.Logger.Error("error in handle topic:", err)
		return fmt.Errorf("error in handle topic: %v", err)
	}
	return nil
}

func handleTopic(db *sql.DB, msg []string, topic string) error {
	if err := godotenv.Load(); err != nil {
		logs.Logger.Error("Error loading .env file: ", err)
		return err
	}

	if topic == os.Getenv("KAFAKA_CONTACT_TOPIC") {
		// Call InsertContact to insert the contacts
		Contactid, err := InsertContacts(db, msg)
		if err != nil {
			logs.Logger.Error("error inserting contact: ", err)
			return fmt.Errorf("error inserting contact: %v", err)
		}
		fmt.Printf("Contactid length: %d\n", len(Contactid))
		logs.Logger.Info(fmt.Sprintf("Contactid length: %d\n", len(Contactid)))
	} else if topic == os.Getenv("KAFAKA_ACTIVITY_TOPIC") {
		activityid, err := InsertContactActivity(db, msg)
		if err != nil {
			logs.Logger.Error("error inserting contact activity: ", err)
			return fmt.Errorf("error inserting contact activity: %v", err)
		}
		logs.Logger.Info(fmt.Sprintf("activityid length: %d\n", len(activityid)))
		fmt.Printf("activityid length: %d\n", len(activityid))
	}

	return nil
}

// InsertContacts inserts multiple contact records into the database in a transaction.
func InsertContacts(db *sql.DB, msg []string) ([]int64, error) {
	tx, err := db.Begin()
	if err != nil {
		logs.Logger.Error("error starting a transaction", err)
		return nil, fmt.Errorf("error starting a transaction: %v", err)
	}
	defer tx.Rollback()

	contactIDs := []int64{}

	contactStatuses, err := types.Extractmsgcontacts(msg)
	if err != nil {
		logs.Logger.Error("error extracting contact statuses", err)
		return nil, fmt.Errorf("error extracting contact statuses: %v", err)
	}

	stmt, err := tx.Prepare("INSERT INTO Contacts (ID,Name, Email, Details, Status) VALUES (?,?, ?, ?, ?)")
	if err != nil {
		logs.Logger.Error("error preparing statement:", err)
		return nil, fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	for _, contactStatus := range contactStatuses {
		lastInsertID, err := insertSingleContactWithTx(stmt, contactStatus.Contact, contactStatus.Status)
		if err != nil {
			logs.Logger.Error("error inserting contact:", err)
			return nil, fmt.Errorf("error inserting contact: %v", err)
		}
		contactIDs = append(contactIDs, lastInsertID)
	}

	if err := tx.Commit(); err != nil {
		logs.Logger.Error("error committing transaction:", err)
		return nil, fmt.Errorf("error committing transaction: %v", err)
	}

	return contactIDs, nil
}

func insertSingleContactWithTx(stmt *sql.Stmt, contact types.Contacts, status int) (int64, error) {
	res, err := stmt.Exec(contact.ID, contact.Name, contact.Email, contact.Details, status)
	if err != nil {
		logs.Logger.Error("error:", err)
		return 0, err
	}

	lastInsertID, err := res.LastInsertId()
	if err != nil {
		logs.Logger.Error("error:", err)
		return 0, err
	}

	return lastInsertID, nil
}

func InsertContactActivity(db *sql.DB, msg []string) ([]string, error) {
	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		logs.Logger.Error("error starting a transation:", err)
		return nil, fmt.Errorf("error starting a transaction: %v", err)
	}
	defer tx.Rollback()

	contactIDs := []string{}

	contactActivity, err := types.ExtractmsgActivity(msg)
	if err != nil {
		logs.Logger.Error("error extracting contact stauses:", err)
		return nil, fmt.Errorf("error extracting contact statuses: %v", err)
	}

	// Perform the insert operation with the transaction
	for _, activity := range contactActivity {
		lastInsertID, err := InsertSingleContactActivityWithTx(tx, activity)
		if err != nil {
			logs.Logger.Error("error inserting contact:", err)
			return nil, fmt.Errorf("error inserting contact: %v", err)
		}
		contactIDs = append(contactIDs, lastInsertID)
	}

	// Commit the transaction if all inserts were successful
	if err := tx.Commit(); err != nil {
		logs.Logger.Error("error commiting transaction:", err)
		return nil, fmt.Errorf("error committing transaction: %v", err)
	}

	return contactIDs, nil
}
func InsertSingleContactActivityWithTx(tx *sql.Tx, activity types.ContactActivity) (string, error) {
	// Insert a single contact using the provided transaction
	result, err := tx.Exec("INSERT INTO ContactActivity (ContactsID, CampaignID, ActivityType, ActivityDate) VALUES (?, ?, ?, ?)",
		activity.ContactID, activity.CampaignID, activity.ActivityType, activity.ActivityDate)
	if err != nil {
		logs.Logger.Error("error:", err)
		return "", err
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		logs.Logger.Error("error:", err)
		return "", err
	}

	lastInsertIDStr := strconv.FormatInt(lastInsertID, 10) // Convert int64 to string
	return lastInsertIDStr, nil
}
