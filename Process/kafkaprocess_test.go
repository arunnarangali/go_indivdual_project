package Process_test

import (
	"datastream/Process"
	"fmt"
	"testing"
)

func TestRunKafkaProducerContacts(t *testing.T) {
	t.Run("EmptyMessages", func(t *testing.T) {
		messages := []string{}
		err := Process.RunKafkaProducerContacts(messages)

		if err == nil {
			t.Error("Expected an error, got nil")
		}

		expectedErrorMsg := "message string is empty"
		if err.Error() != expectedErrorMsg {
			t.Errorf("Expected error message '%s', but got '%s'", expectedErrorMsg, err.Error())
		}

	})

	t.Run("LessThan100Messages", func(t *testing.T) {
		messages := []string{"message1", "message2", "message3", "message4", "message5"}
		err := Process.RunKafkaProducerContacts(messages)

		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}

	})

	t.Run("MoreThan100Messages", func(t *testing.T) {
		messages := make([]string, 110)
		for i := 0; i < 110; i++ {
			messages[i] = fmt.Sprintf("message%d", i+1)
		}

		err := Process.RunKafkaProducerContacts(messages)

		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}

	})

}
