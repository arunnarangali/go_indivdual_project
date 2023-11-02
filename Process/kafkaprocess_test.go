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

func TestRunKafkaProducerActivity(t *testing.T) {
	t.Run("EmptyMessages", func(t *testing.T) {
		messages := []string{}
		err := Process.RunKafkaProducerActivity(messages)

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
		err := Process.RunKafkaProducerActivity(messages)

		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}

	})

	t.Run("MoreThan100Messages", func(t *testing.T) {
		messages := make([]string, 110)
		for i := 0; i < 110; i++ {
			messages[i] = fmt.Sprintf("message%d", i+1)
		}

		err := Process.RunKafkaProducerActivity(messages)

		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}

	})

}

func TestRunKafkaConsumerContacts(t *testing.T) {

	t.Run("Test successful run", func(t *testing.T) {
		go func() {
			err := Process.RunKafkaConsumerContacts("kafka")

			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}()
		messages := []string{`avbssdda,John,johndoe@example.com,1,'{"name":"John","age":30}',
	abcdesddd,Jane,janesmith@example.com,1,'{"name":"John","age":30}`}
		err := Process.RunKafkaProducerContacts(messages)

		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}

	})

	t.Run("Test error in ConfigureKafka", func(t *testing.T) {

		err := Process.RunKafkaConsumerContacts("invalidconfig")
		if err == nil {
			t.Error("Expected an error, but got nil")
		}
		expectedErrorMsg := "unsupported DB_TYPE: invalidconfig"
		if err.Error() != expectedErrorMsg {
			t.Errorf("Expected error message '%s', but got '%s'", expectedErrorMsg, err.Error())
		}

	})

}

func TestRunKafkaConsumerActivity(t *testing.T) {

	t.Run("Test successful run", func(t *testing.T) {
		go func() {
			err := Process.RunKafkaConsumerActivity("kafka")

			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}()
		messages := []string{`4c88502a43d9444690a8dafdba9c8eeaf7d408378919422f,83,4,"2023-11-02 00:00:00 +0000 UTC"`}
		err := Process.RunKafkaProducerActivity(messages)

		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}

	})

	t.Run("Test error in ConfigureKafka", func(t *testing.T) {

		err := Process.RunKafkaConsumerActivity("invalidconfig")
		if err == nil {
			t.Error("Expected an error, but got nil")
		}
		expectedErrorMsg := "unsupported DB_TYPE: invalidconfig"
		if err.Error() != expectedErrorMsg {
			t.Errorf("Expected error message '%s', but got '%s'", expectedErrorMsg, err.Error())
		}

	})

}
