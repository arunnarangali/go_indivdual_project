package service

import (
	"datastream/config"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestNewKafkaConnector(t *testing.T) {
	// Create a test Kafka configuration for success case
	successKafkaConfig := config.KafkaConfig{
		Broker: "localhost:9092", // Example broker address
	}

	// Create a test Kafka configuration for failure case (invalid broker)
	invalidKafkaConfig := config.KafkaConfig{
		Broker: "invalid-broker", // Invalid broker address
	}

	t.Run("Success", func(t *testing.T) {
		// Test a successful creation of KafkaConnector
		connector, err := NewKafkaConnector(successKafkaConfig)

		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		if connector == nil {
			t.Error("Expected a non-nil KafkaConnector, but got nil")
		}

		// You can further test properties of the KafkaConnector if needed.
	})

	t.Run("InvalidBroker", func(t *testing.T) {
		// Test creating KafkaConnector with an invalid broker address
		connector, err := NewKafkaConnector(invalidKafkaConfig)

		if err == nil {
			t.Error("Expected an error, but got nil")
		}

		if connector != nil {
			t.Error("Expected nil KafkaConnector, but got non-nil")
		}

		expectedErrorMsg :=
			"kafka: client has run out of available brokers to talk to: dial tcp: address invalid-broker: missing port in address"
		if err.Error() != expectedErrorMsg {
			t.Errorf("Expected error message '%s', but got '%s'", expectedErrorMsg, err.Error())
		}
		fmt.Println(err.Error())
	})
}

func TestKafkaConnector_ProduceMessages(t *testing.T) {
	t.Run("Successful Message Production", func(t *testing.T) {
		testkafkaConfig := config.KafkaConfig{
			Broker: "localhost:9092",
		}

		kc, err := NewKafkaConnector(testkafkaConfig)
		if err != nil {
			t.Fatalf("Error creating KafkaConnector: %v", err)
		}
		defer kc.Close() // Close the KafkaConnector after the test

		// Define test data
		topic := "test-topic"
		messageValues := []string{"message1", "message2"}

		// Test message production
		err = kc.ProduceMessages(topic, messageValues)
		if err != nil {
			t.Fatalf("Error producing messages: %v", err)
		}
	})

	t.Run("Message Production Error", func(t *testing.T) {
		testkafkaConfig := config.KafkaConfig{
			Broker: "localhost:9092",
		}
		kc, err := NewKafkaConnector(testkafkaConfig)
		if err != nil {
			t.Fatalf("Error creating KafkaConnector: %v", err)
		}
		defer kc.Close() // Close the KafkaConnector after the test

		// Define test data
		topic := "test-topic"
		// Invalid messageValues to trigger an error
		messageValues := []string{}

		// Test message production (expecting an error)
		err = kc.ProduceMessages(topic, messageValues)
		if err == nil {
			t.Fatal("Expected an error but got nil")
		}
	})

	t.Run("Producer and Consumer Interaction", func(t *testing.T) {
		kafkaConfig := config.KafkaConfig{
			Broker: "localhost:9092", // You can change this to your test Kafka broker address
		}
		kc, err := NewKafkaConnector(kafkaConfig)
		if err != nil {
			t.Fatalf("Error creating KafkaConnector: %v", err)
		}
		defer kc.Close()

		// Define your test data
		topic := "test-topic"
		messageValues := []string{"message1"}
		// Optionally, you can consume the messages to verify they were produced
		consumer, err := sarama.NewConsumer([]string{kafkaConfig.Broker}, nil)
		if err != nil {
			t.Fatalf("Error creating Kafka consumer: %v", err)
		}
		defer consumer.Close()

		partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if err != nil {
			t.Fatalf("Error creating partition consumer: %v", err)
		}
		defer partitionConsumer.Close()
		// Produce messages
		err = kc.ProduceMessages(topic, messageValues)
		if err != nil {
			t.Fatalf("Error producing messages: %v", err)
		}

		// Consume messages and validate
		for i, expectedMessage := range messageValues {
			select {
			case message := <-partitionConsumer.Messages():
				if string(message.Value) != expectedMessage {
					t.Errorf("Expected message %d to be '%s', but got '%s'", i, expectedMessage, string(message.Value))
				}
			case <-time.After(2 * time.Second):
				t.Errorf("Timeout waiting for message %d", i)
			}
		}
	})
}

func TestKafkaConnector_ConsumeMessages(t *testing.T) {
	t.Run("Successful Message Consumption", func(t *testing.T) {

		testkafkaConfig := config.KafkaConfig{
			Broker: "localhost:9092",
		}
		kc, err := NewKafkaConnector(testkafkaConfig)
		if err != nil {
			t.Fatalf("Error creating KafkaConnector: %v", err)
		}
		defer kc.Close()

		topic := "test-topic"

		done := make(chan struct{})
		go func() {
			defer close(done)
			err := kc.ConsumeMessages(topic)
			if err != nil {
				t.Errorf("Error consuming messages: %v", err)
			}
		}()

		time.Sleep(5 * time.Second)
	})

	t.Run("Consumer Error Handling", func(t *testing.T) {

		testkafkaConfig := config.KafkaConfig{
			Broker: "localhost:9092",
		}

		kc, err := NewKafkaConnector(testkafkaConfig)
		if err != nil {
			t.Fatalf("Error creating KafkaConnector: %v", err)
		}
		defer kc.Close()

		topic := "test-topic"

		// Simulate a consumer error

		done := make(chan struct{})
		go func() {
			defer close(done)
			err := kc.ConsumeMessages(topic)
			if err == nil {
				t.Errorf("Expected a consumer error, but got nil")
			}
		}()

		time.Sleep(5 * time.Second)
	})
}

func TestConfigureKafka(t *testing.T) {
	// Test case 1: Valid configuration
	t.Run("Valid Configuration", func(t *testing.T) {
		// Create a test Kafka configuration
		testKafkaConfig := config.KafkaConfig{
			Broker: "localhost:9092",
			Topic1: "ActivityContactTopicNew53",
			Topic2: "ContactStatusTopicNew54",
		}

		// Test the ConfigureKafka function
		kafkaConnector, contactTopic, activityTopic, err := ConfigureKafka("kafka")
		if err != nil {
			t.Fatalf("Error configuring Kafka: %v", err)
		}

		if kafkaConnector == nil {
			t.Fatal("KafkaConnector is nil")
		}
		if *contactTopic != testKafkaConfig.Topic2 {
			t.Errorf("Expected contactTopic to be '%s', but got '%s'", testKafkaConfig.Topic2, *contactTopic)
		}
		if *activityTopic != testKafkaConfig.Topic1 {
			t.Errorf("Expected activityTopic to be '%s', but got '%s'", testKafkaConfig.Topic1, *activityTopic)
		}
	})

	// Test case 2: Error when loading Kafka configuration
	t.Run("Error loading Kafka configuration", func(t *testing.T) {

		// Inject an error by providing an invalid Kafka configuration type
		_, _, _, err := ConfigureKafka("wrongConfigMsg")

		if err == nil {
			t.Error("Expected an error, got nil")
		}
		expectedErrorMsg :=
			"unsupported DB_TYPE: wrongConfigMsg"
		if err.Error() != expectedErrorMsg {
			t.Errorf("Expected error message '%s', but got '%s'", expectedErrorMsg, err.Error())
		}
	})

	// Test case 3: Error when creating KafkaConnector
	t.Run("Missing Configuration Data", func(t *testing.T) {

		// Set up your test environment to simulate missing configuration data.
		_, _, _, err := ConfigureKafka("")

		if err == nil {
			t.Error("Expected an error, got nil")
		}
		expectedErrorMsg := "configmsg is nil"
		if err.Error() != expectedErrorMsg {
			t.Errorf("Expected error message '%s', but got '%s'", expectedErrorMsg, err.Error())
		}
	})
}

func TestRunKafkaProducerContacts(t *testing.T) {
	// Set up the global Kafka configurations
	kafkaConnector, contactTopic, _, _ := ConfigureKafka("kafka")

	// Test case 1: Producing Messages Successfully
	t.Run("Producing Messages Successfully", func(t *testing.T) {
		messages := []string{"message1", "message2", "message3"}

		// Reset the global Kafka configurations just for this test
		origKafkaConnector := kafkaConnector
		origcontactTopic := contactTopic

		kafkaConnector, contactTopic, _, _ = ConfigureKafka("kafka")

		err := RunKafkaProducerContacts(messages)

		// Restore the original Kafka configurations
		kafkaConnector, contactTopic = origKafkaConnector, origcontactTopic

		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}
	})

	// Test case 2: Kafka Connection Error
	t.Run("Kafka Connection Error", func(t *testing.T) {
		// Simulate a Kafka connection error
		origKafkaConnector := kafkaConnector
		kafkaConnector = nil

		messages := []string{"message1", "message2", "message3"}

		err := RunKafkaProducerContacts(messages)

		// Restore the original Kafka configurations
		kafkaConnector = origKafkaConnector

		if err == nil {
			t.Error("Expected an error, but got none.")
		}
	})

	// Test case 3: Producing Large Number of Messages
	t.Run("Producing Large Number of Messages", func(t *testing.T) {
		messages := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			messages[i] = fmt.Sprintf("message%d", i)
		}

		// Reset the global Kafka configurations just for this test
		origKafkaConnector := kafkaConnector
		origcontactTopic := contactTopic
		kafkaConnector, contactTopic, _, _ = ConfigureKafka("kafka")

		err := RunKafkaProducerContacts(messages)

		// Restore the original Kafka configurations
		kafkaConnector, contactTopic = origKafkaConnector, origcontactTopic

		if err != nil {
			t.Errorf("Expected no error, but got: %v", err)
		}
	})
}
