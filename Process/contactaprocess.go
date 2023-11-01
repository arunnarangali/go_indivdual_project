package Process

import (
	"database/sql"
	"datastream/logs"
	"datastream/service"
	"datastream/types"
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

func Insertmsg(msg []string, topic string) error {

	dbConnector, err := service.ConfigureMySQLDB("mysql")
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

	err = HandleTopic(db, msg, topic)

	if err != nil {
		logs.Logger.Error("error in handle topic:", err)
		return fmt.Errorf("error in handle topic: %v", err)
	}

	return nil
}

func HandleTopic(db *sql.DB, msg []string, topic string) error {

	if err := godotenv.Load("/home/arun/test 7/go_indivdual_project/.env"); err != nil {
		logs.Logger.Error("Error loading .env file: ", err)
		return err
	}

	if topic == os.Getenv("KAFAKA_CONTACT_TOPIC") {

		err := ExtractContactsFromMessage(db, msg)

		if err != nil {
			logs.Logger.Error("error inserting contact: ", err)
			return fmt.Errorf("error inserting contact: %v", err)
		}

	} else if topic == os.Getenv("KAFAKA_ACTIVITY_TOPIC") {

		err := ExtractActivityFromMessage(db, msg)

		if err != nil {
			logs.Logger.Error("error inserting contact activity: ", err)
			return fmt.Errorf("error inserting contact activity: %v", err)
		}

	} else {
		logs.Logger.Error("error ", fmt.Errorf("invalid Topic"))
		return fmt.Errorf("invalid Topic")
	}

	return nil
}

func ExtractContactsFromMessage(db *sql.DB, msg []string) error {
	contactStatuses, err := types.Extractmsgcontacts(msg)
	if err != nil {
		logs.Logger.Error("error extracting contact statuses:", err)
		return err
	}

	tableName := "Contacts"
	columnNames := []string{"ID", "Name", "Email", "Details", "Status"}

	contactData := make([][]interface{}, len(contactStatuses))
	for i, status := range contactStatuses {
		contactData[i] = []interface{}{
			status.Contact.ID,
			status.Contact.Name,
			status.Contact.Email,
			status.Contact.Details,
			status.Status,
		}
	}

	contactIDs, err := service.InsertDataToMySql(db, tableName, columnNames, contactData)
	if err != nil {
		logs.Logger.Error("error inserting contact activity:", err)
		return err
	}

	logs.Logger.Info(fmt.Sprintf("ContactID length: %d\n", len(contactIDs)))
	fmt.Printf("ContactId length: %d\n", len(contactIDs))
	return err
}

func ExtractActivityFromMessage(db *sql.DB, msg []string) error {
	activity, err := types.ExtractmsgActivity(msg)
	if err != nil {
		logs.Logger.Error("error extracting activity:", err)
		return err
	}

	tableName := "ContactActivity"
	columnNames := []string{"ContactsID", "CampaignID", "ActivityType", "ActivityDate"}

	activityData := make([][]interface{}, len(activity))
	for i, status := range activity {
		activityData[i] = []interface{}{
			status.ContactID,
			status.CampaignID,
			status.ActivityType,
			status.ActivityDate,
		}
	}

	activityIDs, err := service.InsertDataToMySql(db, tableName, columnNames, activityData)
	if err != nil {
		logs.Logger.Error("error inserting contact activity:", err)
		return err
	}

	logs.Logger.Info(fmt.Sprintf("activityid length: %d\n", len(activityIDs)))
	fmt.Printf("activityid length: %d\n", len(activityIDs))
	return err
}
