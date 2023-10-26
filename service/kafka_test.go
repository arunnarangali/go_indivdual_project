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
