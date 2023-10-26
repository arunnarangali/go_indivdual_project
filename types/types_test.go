package types

import (
	"reflect"
	"testing"
	"time"
)

func TestExtractmsgcontacts(t *testing.T) {
	t.Run("Valid Input Case", func(t *testing.T) {
		input := []string{
			"1,John Doe,johndoe@example.com,2,Some details",
			"2,Jane Smith,janesmith@example.com,1,Another detail",
		}
		expected := []ContactStatus{
			{
				Contact: Contacts{
					ID:      "1",
					Name:    "John Doe",
					Email:   "johndoe@example.com",
					Details: "Some details",
				},
				Status: 2,
			},
			{
				Contact: Contacts{
					ID:      "2",
					Name:    "Jane Smith",
					Email:   "janesmith@example.com",
					Details: "Another detail",
				},
				Status: 1,
			},
		}

		result, err := Extractmsgcontacts(input)
		if err != nil {
			t.Errorf("Expected no error, but got an error: %v", err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Result does not match the expected value.")
		}
	})

	t.Run("Valid Input Case with Missing Details", func(t *testing.T) {
		input := []string{
			"1,John Doe,johndoe@example.com,2,Some details",
			"2,Jane Smith,janesmith@example.com,1",
		}
		expected := []ContactStatus{
			{
				Contact: Contacts{
					ID:      "1",
					Name:    "John Doe",
					Email:   "johndoe@example.com",
					Details: "Some details",
				},
				Status: 2,
			},
		}

		result, err := Extractmsgcontacts(input)
		if err != nil {
			t.Errorf("Expected no error, but got an error: %v", err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Result does not match the expected value.")
		}
	})

	t.Run("Invalid Status Format Case", func(t *testing.T) {
		input := []string{
			"1,John Doe,johndoe@example.com,2,Some details",
			"2,Jane Smith,janesmith@example.com,invalid,Another detail",
		}

		_, err := Extractmsgcontacts(input)
		if err == nil {
			t.Error("Expected an error, but got no error.")
		}
	})

	t.Run("Empty Input Case", func(t *testing.T) {
		input := []string{}
		expected := []ContactStatus{}

		result, err := Extractmsgcontacts(input)
		if err != nil {
			t.Errorf("Expected no error, but got an error: %v", err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Result does not match the expected value.")
		}
	})

	t.Run("Empty Lines in Messages", func(t *testing.T) {
		input := []string{
			"1,John Doe,johndoe@example.com,2,Some details",
			"",
			"2,Jane Smith,janesmith@example.com,1,Another detail",
		}
		expected := []ContactStatus{
			{
				Contact: Contacts{
					ID:      "1",
					Name:    "John Doe",
					Email:   "johndoe@example.com",
					Details: "Some details",
				},
				Status: 2,
			},
			{
				Contact: Contacts{
					ID:      "2",
					Name:    "Jane Smith",
					Email:   "janesmith@example.com",
					Details: "Another detail",
				},
				Status: 1,
			},
		}

		result, err := Extractmsgcontacts(input)
		if err != nil {
			t.Errorf("Expected no error, but got an error: %v", err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Result does not match the expected value.")
		}
	})

	t.Run("Malformed Message Lines", func(t *testing.T) {
		input := []string{
			"1,John Doe,johndoe@example.com,2,Some details",
			"2,Jane Smith,janesmith@example.com",
		}
		expected := []ContactStatus{
			{
				Contact: Contacts{
					ID:      "1",
					Name:    "John Doe",
					Email:   "johndoe@example.com",
					Details: "Some details",
				},
				Status: 2,
			},
		}

		result, err := Extractmsgcontacts(input)
		if err != nil {
			t.Errorf("Expected no error, but got an error: %v", err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Result does not match the expected value.")
		}
	})
}

func TestExtractmsgActivity(t *testing.T) {
	t.Run("Valid Input Case", func(t *testing.T) {
		input := []string{
			"1,123,1,\"2023-10-26 10:00:00 -0700 MST\"",
			"2,456,2,\"2023-10-27 14:30:00 -0700 MST\"",
		}
		expected := []ContactActivity{
			{
				ContactID:    "1",
				CampaignID:   123,
				ActivityType: 1,
				ActivityDate: time.Date(2023, 10, 26, 10, 0, 0, 0, time.FixedZone("MST", -7*3600)),
			},
			{
				ContactID:    "2",
				CampaignID:   456,
				ActivityType: 2,
				ActivityDate: time.Date(2023, 10, 27, 14, 30, 0, 0, time.FixedZone("MST", -7*3600)),
			},
		}

		result, err := ExtractmsgActivity(input)
		if err != nil {
			t.Errorf("Expected no error, but got an error: %v", err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Result does not match the expected value.")
		}
	})

	t.Run("Valid Input Case with Extra Spaces", func(t *testing.T) {
		input := []string{
			"  1,  123,  1,  \"2023-10-26 10:00:00 -0700 MST\"",
			"2, 456, 2, \"2023-10-27 14:30:00 -0700 MST\"  ",
		}
		expected := []ContactActivity{
			{
				ContactID:    "1",
				CampaignID:   123,
				ActivityType: 1,
				ActivityDate: time.Date(2023, 10, 26, 10, 0, 0, 0, time.FixedZone("MST", -7*3600)),
			},
			{
				ContactID:    "2",
				CampaignID:   456,
				ActivityType: 2,
				ActivityDate: time.Date(2023, 10, 27, 14, 30, 0, 0, time.FixedZone("MST", -7*3600)),
			},
		}

		result, err := ExtractmsgActivity(input)
		if err != nil {
			t.Errorf("Expected no error, but got an error: %v", err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Result does not match the expected value.")
		}
	})

	t.Run("Invalid Date Format Case", func(t *testing.T) {
		input := []string{
			"1,123,1,\"Invalid Date Format\"",
			"2,456,2,\"Invalid Date Format\"",
		}

		_, err := ExtractmsgActivity(input)
		if err == nil {
			t.Error("Expected an error, but got no error.")
		}
	})

	t.Run("Empty Input Case", func(t *testing.T) {
		input := []string{}
		expected := []ContactActivity{}

		result, err := ExtractmsgActivity(input)
		if err != nil {
			t.Errorf("Expected no error, but got an error: %v", err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Result does not match the expected value.")
		}
	})

	t.Run("Empty Lines in Messages", func(t *testing.T) {
		input := []string{
			"1,123,1,\"2023-10-26 10:00:00 -0700 MST\"",
			"",
			"2,456,2,\"2023-10-27 14:30:00 -0700 MST\"",
		}
		expected := []ContactActivity{
			{
				ContactID:    "1",
				CampaignID:   123,
				ActivityType: 1,
				ActivityDate: time.Date(2023, 10, 26, 10, 0, 0, 0, time.FixedZone("MST", -7*3600)),
			},
			{
				ContactID:    "2",
				CampaignID:   456,
				ActivityType: 2,
				ActivityDate: time.Date(2023, 10, 27, 14, 30, 0, 0, time.FixedZone("MST", -7*3600)),
			},
		}

		result, err := ExtractmsgActivity(input)
		if err != nil {
			t.Errorf("Expected no error, but got an error: %v", err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Result does not match the expected value.")
		}
	})

	t.Run("Malformed Message Lines", func(t *testing.T) {
		input := []string{
			"1,123,1,\"2023-10-26 10:00:00 -0700 MST\"",
			"2,456,2",
		}
		expected := []ContactActivity{
			{
				ContactID:    "1",
				CampaignID:   123,
				ActivityType: 1,
				ActivityDate: time.Date(2023, 10, 26, 10, 0, 0, 0, time.FixedZone("MST", -7*3600)),
			},
		}

		result, err := ExtractmsgActivity(input)
		if err != nil {
			t.Errorf("Expected no error, but got an error: %v", err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Result does not match the expected value.")
		}
	})
}
