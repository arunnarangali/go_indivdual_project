package dataprocessing

import (
	"datastream/types"
	"testing"
)

func TestCallActivity(t *testing.T) {

	contacts := types.Contacts{
		ID:      "1",
		Name:    "John Doe",
		Email:   "john.doe@example.com",
		Details: "Test details",
	}

	t.Run("TestCase1", func(t *testing.T) {
		contactStatus, activitiesString := CallActivity("123", contacts)

		if contactStatus == "" {
			t.Errorf("Expected non-empty contactStatus, but got an empty string")
		}
		if activitiesString == "" {
			t.Errorf("Expected non-empty activitiesString, but got an empty string")
		}
	})

}

func BenchmarkCallActivity(b *testing.B) {

	contacts := types.Contacts{
		ID:      "1",
		Name:    "John Doe",
		Email:   "john.doe@example.com",
		Details: "Test details",
	}

	for i := 0; i < b.N; i++ {

		_, _ = CallActivity("123", contacts)
	}
}
