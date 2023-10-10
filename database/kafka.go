package database

import (
	"datastream/service"
	"fmt"
)

func RunKafkaProducerContacts(msg string) error {

	kafkaConnector, contactTopic, _, err := service.ConfigureKafka()
	if err != nil {
		return err
	}
	messageValue := msg // Set the message value to the provided 'msg' parameter.
	// Produce a message to the Kafka topic.
	if err := kafkaConnector.ProduceMessage(*contactTopic, messageValue); err != nil {
		return err
	}
	// Close the KafkaConnector when done.
	if err := kafkaConnector.Close(); err != nil {
		return err
	}
	fmt.Println("Message successfully produced to Kafka.")
	return nil
}

func RunKafkaConsumerContacts() error {

	kafkaConnector, contactTopic, _, err := service.ConfigureKafka()
	if err != nil {
		return err
	}
	// Consume messages from the Kafka topic.
	if err := kafkaConnector.ConsumeMessages(*contactTopic); err != nil {
		return err
	}
	// Close the KafkaConnector when done (this will only be reached after receiving a signal to stop).
	if err := kafkaConnector.Close(); err != nil {
		return err
	}
	return nil
}

func RunKafkaProducerActivity(msg string) error {

	kafkaConnector, _, activityTopic, err := service.ConfigureKafka()
	if err != nil {
		return err
	}
	messageValue := msg // Set the message value to the provided 'msg' parameter.
	// Produce a message to the Kafka topic.
	if err := kafkaConnector.ProduceMessage(*activityTopic, messageValue); err != nil {
		// If there was an error producing the message, return the error.
		return err
	}
	// Close the KafkaConnector when done.
	if err := kafkaConnector.Close(); err != nil {
		return err
	}
	fmt.Println("Message successfully produced to Kafka.")
	return nil
}

func RunKafkaConsumerActivity() error {

	kafkaConnector, _, activityTopic, err := service.ConfigureKafka()
	if err != nil {
		return err
	}
	// Consume messages from the Kafka topic.
	if err := kafkaConnector.ConsumeMessages(*activityTopic); err != nil {
		return err
	}

	// Close the KafkaConnector when done (this will only be reached after receiving a signal to stop).
	if err := kafkaConnector.Close(); err != nil {
		return err
	}

	return nil
}
