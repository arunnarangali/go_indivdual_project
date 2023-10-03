package types

type Contacts struct {
	ID      int
	Name    string
	Email   string
	Details string
}

type ContactActivity struct {
	ID           int
	ContactID    int
	CampaignID   int
	ActivityType int
	ActivityDate string
	// Add any other fields you need here
}

type ContactStatus struct {
	Contact Contacts
	Status  int
}

// ContactFactory interface
type ContactFactory interface {
	CreateContactActivity(id int, contactid int, campaignid int, activitytytype int, activitydate string) ContactActivity
	CreateContactStatus(id int, name string, email string, details string, status int) ContactStatus
}

// DefaultContactFactory struct
type DefaultContactFactory struct{}

func (f DefaultContactFactory) CreateContactActivity(id int, contactid int, campaignid int, activitytytype int, activitydate string) ContactActivity {
	return ContactActivity{
		ID:           id,
		ContactID:    contactid,
		CampaignID:   campaignid,
		ActivityType: activitytytype,
		ActivityDate: activitydate,
	}
}

func (f DefaultContactFactory) CreateContactStatus(id int, name string, email string, details string, status int) ContactStatus {
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

type QueryOutput struct {
}
