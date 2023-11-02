package types

import (
	"datastream/logs"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Contacts struct {
	ID      string
	Name    string
	Email   string
	Details string
}

type ContactActivity struct {
	ID           int
	ContactID    string
	CampaignID   int
	ActivityType int
	ActivityDate time.Time
	// Add any other fields you need here
}

type ContactStatus struct {
	Contact Contacts
	Status  int
}

// ContactFactory interface
type ContactFactory interface {
	CreateContactActivity(id int, contactid int, campaignid int, activitytytype int, activitydate time.Time) ContactActivity
	CreateContactStatus(id int, name string, email string, details string, status int) ContactStatus
}

// DefaultContactFactory struct
type DefaultContactFactory struct{}

func (f DefaultContactFactory) CreateContactActivity(id int, contactid string, campaignid int,
	activitytytype int, activitydate time.Time) ContactActivity {
	return ContactActivity{
		ID:           id,
		ContactID:    contactid,
		CampaignID:   campaignid,
		ActivityType: activitytytype,
		ActivityDate: activitydate,
	}
}

func (f DefaultContactFactory) CreateContactStatus(id string, name string, email string, details string, status int) ContactStatus {
	contact := Contacts{
		ID:      id,
		Name:    name,
		Email:   email,
		Details: details,
	}

	return ContactStatus{
		Contact: contact,
		Status:  status,
	}
}
func Convertmsgcontacts(msg []string) ([]ContactStatus, error) {
	contactStatuses := []ContactStatus{}
	for _, message := range msg {

		lines := strings.Split(message, "\n")

		for _, line := range lines {
			values := strings.SplitN(line, ",", 5)

			if len(values) >= 5 {
				id := strings.TrimSpace(values[0])
				name := strings.TrimSpace(values[1])
				email := strings.TrimSpace(values[2])
				statusStr := strings.TrimSpace(values[3])
				detailsStr := strings.TrimSpace(values[4])

				fmt.Printf("Processing message:id=%s , name=%s, email=%s, details=%s, status=%s\n",
					id, name, email, detailsStr, statusStr)

				status, err := strconv.Atoi(statusStr)
				if err != nil {
					logs.Logger.Error("invalid status format:", err)
					return nil, fmt.Errorf("invalid status format: %v", err)
				}

				contactStatus := ContactStatus{
					Contact: Contacts{
						ID:      id,
						Name:    name,
						Email:   email,
						Details: detailsStr,
					},
					Status: status,
				}
				contactStatuses = append(contactStatuses, contactStatus)
			}
		}
	}
	return contactStatuses, nil
}

func ConvertmsgActivity(msg []string) ([]ContactActivity, error) {
	contactStatuses := []ContactActivity{} // Corrected declaration to create a slice
	fmt.Printf("this is Acticitystring:%s", msg)

	for _, message := range msg {

		lines := strings.Split(message, "\n")
		for _, line := range lines {
			values := strings.Split(line, ",")

			if len(values) >= 4 {
				contactsid := strings.TrimSpace(values[0])
				campaignidStr := strings.TrimSpace(values[1])
				campaignid, _ := strconv.Atoi(campaignidStr)

				activitytypeStr := strings.TrimSpace(values[2])
				activitytype, _ := strconv.Atoi(activitytypeStr)

				activitydateStr := strings.TrimSpace(values[3])

				// Remove double quotes from activitydateStr.
				activitydateStr = strings.Trim(activitydateStr, `"`)
				layout := "2006-01-02 15:04:05 -0700 MST"
				// Parse 'activitydate' string to a time.Time object.
				activitydate, err := time.Parse(layout, activitydateStr)
				if err != nil {
					logs.Logger.Error("error in activitydate parsing", err)
					return nil, err
				}

				fmt.Printf("Processing message:contactid=%s, campaindid=%d, activitytype=%d, activitydate=%s\n",
					contactsid, campaignid, activitytype, activitydate)

				contactactivity := ContactActivity{
					ContactID:    contactsid,
					CampaignID:   campaignid,
					ActivityType: activitytype,
					ActivityDate: activitydate,
				}
				contactStatuses = append(contactStatuses, contactactivity)
			}
		}
	}
	return contactStatuses, nil
}

type QueryOutput struct {
	ContactID string
	Click     int
}
