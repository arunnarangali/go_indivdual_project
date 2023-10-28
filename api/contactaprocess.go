package api

import (
	"database/sql"
	"datastream/logs"
	"datastream/types"
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func HandleTopic(db *sql.DB, msg []string, topic string) error {

	if err := godotenv.Load("/home/arun/test 5/go_indivdual_project/.env"); err != nil {
		logs.Logger.Error("Error loading .env file: ", err)
		return err
	}

	if topic == os.Getenv("KAFAKA_CONTACT_TOPIC") {

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
	} else {
		logs.Logger.Error("error ", fmt.Errorf("invalid Topic"))
		return fmt.Errorf("invalid Topic")
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
		return nil, err
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

	activityIDs := []string{}

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
		activityIDs = append(activityIDs, lastInsertID)
	}

	// Commit the transaction if all inserts were successful
	if err := tx.Commit(); err != nil {
		logs.Logger.Error("error commiting transaction:", err)
		return nil, fmt.Errorf("error committing transaction: %v", err)
	}

	return activityIDs, nil
}

func InsertSingleContactActivityWithTx(tx *sql.Tx, activity types.ContactActivity) (string, error) {

	result, err := tx.Exec(
		"INSERT INTO ContactActivity (ContactsID, CampaignID, ActivityType, ActivityDate) VALUES (?, ?, ?, ?)",
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
