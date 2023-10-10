package dataprocessing

import (
	"datastream/types"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

var activityDate1, activityDate2, activityDate3 time.Time
var activityDateX, activityDate time.Time
var i int
var flag int
var ActivityString string
var factory = types.DefaultContactFactory{}
var ContactActivities []types.ContactActivity // Slice to store ContactActivity instances

func calculateActivityDate() {
	activityDate1 = activityDate.AddDate(0, 0, 1)
	activityDate2 = activityDate1.AddDate(0, 0, 2)
	activityDate3 = activityDate2.AddDate(0, 0, 3)
}

func calculateActivity(id int) {
	percent := rand.Intn(101)
	if percent <= 80 {
		if percent <= 30 {
			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
		} else if percent <= 60 {
			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate2
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
		} else {
			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate2
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
			activityDate = activityDate3
			ActivityString += fmt.Sprintf("(%d, %d, 7, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 7, activityDate.Format("2006-01-02"))
		}
	} else if percent <= 90 {
		if percent <= 82 {
			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate2
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
		} else if percent <= 84 {
			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate2
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate3
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
		} else if percent <= 86 {
			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate2
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
			activityDate = activityDate3
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
		} else if percent <= 88 {
			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
			activityDate = activityDate2
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate3
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
		} else if percent <= 89 {
			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate1
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
			activityDate = activityDate2
			ActivityString += fmt.Sprintf("(%d, %d, 7, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 7, activityDate.Format("2006-01-02"))
			activityDate = activityDate3
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
		} else {
			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
			activityDate = activityDate1
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			ActivityString += fmt.Sprintf("(%d, %d, 7, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 7, activityDate.Format("2006-01-02"))
			activityDate = activityDate2
			ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
			activityDate = activityDate3
			ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
		}
	} else {
		percent := rand.Intn(1001)
		if percent <= 940 {
			ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
			appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
		} else {
			flag = 0
			if percent <= 960 {
				ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
				activityDate = activityDate1
				ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
				activityDate = activityDate2
				ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
				activityDate = activityDate3
				ActivityString += fmt.Sprintf("(%d, %d, 5, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 5, activityDate.Format("2006-01-02"))
			} else if percent <= 970 {
				ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
				activityDate = activityDate1
				ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
				activityDate = activityDate2
				ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
				activityDate = activityDate3
				ActivityString += fmt.Sprintf("(%d, %d, 6, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 6, activityDate.Format("2006-01-02"))
			} else if percent <= 980 {
				ActivityString += fmt.Sprintf("(%d, %d, 1, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 1, activityDate.Format("2006-01-02"))
				activityDate = activityDate1
				ActivityString += fmt.Sprintf("(%d, %d, 3, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 3, activityDate.Format("2006-01-02"))
				activityDate = activityDate2
				ActivityString += fmt.Sprintf("(%d, %d, 4, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 4, activityDate.Format("2006-01-02"))
				activityDate = activityDate3
				ActivityString += fmt.Sprintf("(%d, %d, 5, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				ActivityString += fmt.Sprintf("(%d, %d, 6, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 5, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 6, activityDate.Format("2006-01-02"))
			} else {
				ActivityString += fmt.Sprintf("(%d, %d, 2, \"%s\"),", id, i, activityDate.Format("2006-01-02"))
				appendContactActivity(id, i, 2, activityDate.Format("2006-01-02"))
			}
		}
	}
}

func generateData(id int, contacts types.Contacts) types.ContactStatus {
	i++
	var Contacts types.ContactStatus
	if i%20 == 0 {
		activityDateX = activityDateX.AddDate(0, 1, 0)
		activityDate = activityDateX
		calculateActivityDate()
	} else {
		activityDate = activityDateX
	}
	calculateActivity(id)
	if i == 100 || flag == 0 {
		ActivityString = ActivityString[:len(ActivityString)-1]
		if flag == 0 {
			Contacts = factory.CreateContactStatus(contacts.ID, contacts.Name, contacts.Email, contacts.Details, 0)
		} else {
			Contacts = factory.CreateContactStatus(contacts.ID, contacts.Name, contacts.Email, contacts.Details, 1)
		}
	} else {
		Contacts = generateData(id, contacts)

	}
	return Contacts
}

func appendContactActivity(contactID, campaignID, activityType int, activityDate string) {
	// Create a ContactActivity instance and append it to the slice
	activity := types.DefaultContactFactory{}.CreateContactActivity(
		len(ContactActivities)+1, contactID, campaignID, activityType, activityDate)
	ContactActivities = append(ContactActivities, activity)
}

func CallActivity(id int, contacts types.Contacts) (string, string) {
	activityDateX, _ = time.Parse("2006-01-02", "2023-01-01")
	activityDate = activityDateX
	calculateActivityDate()
	i = 0
	flag = 1
	ActivityString = ""
	ContactActivities = nil // Initialize the slice to an empty state
	result := generateData(id, contacts)

	// Print ContactStatus
	contactStatus := fmt.Sprintf("%s, %s, %d,'%s'\n", result.Contact.Name, result.Contact.Email, result.Status, result.Contact.Details)

	activityStrings := make([]string, len(ContactActivities))
	// Print ContactActivities slice
	for index, activity := range ContactActivities {
		activityStrings[index] = fmt.Sprintf("%d,%d,%d,\"%s\",\n", activity.ContactID, activity.CampaignID, activity.ActivityType, activity.ActivityDate)
	}
	// Join the activity strings into a single string
	activitiesString := strings.Join(activityStrings, "")

	return contactStatus, activitiesString

}
