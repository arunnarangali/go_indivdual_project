package database

import (
	"datastream/logs"
	"datastream/service"
	"fmt"
)

func RunKafkaProducerContacts(messages []string) error {

	kafkaConnector, contactTopic, _, err := service.ConfigureKafka()
	if err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}
	// Produce a message to the Kafka topic.
	if err := kafkaConnector.ProduceMessages(*contactTopic, messages); err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}
	// Close the KafkaConnector when done.
	if err := kafkaConnector.Close(); err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}
	fmt.Println("Messages successfully produced to Kafka.")
	return nil
}

func RunKafkaConsumerContacts() error {

	kafkaConnector, contactTopic, _, err := service.ConfigureKafka()
	if err != nil {
		logs.Logger.Error("error in configure:", err)
		return err
	}
	// Consume messages from the Kafka topic.
	if err := kafkaConnector.ConsumeMessages(*contactTopic); err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}
	// Close the KafkaConnector when done (this will only be reached after receiving a signal to stop).
	if err := kafkaConnector.Close(); err != nil {
		logs.Logger.Error("err in kafkaclose", err)
		return err
	}
	return nil
}

func RunKafkaProducerActivity(messages []string) error {

	kafkaConnector, _, activityTopic, err := service.ConfigureKafka()
	if err != nil {
		logs.Logger.Error("error:", err)
		return err
	}

	// Produce a message to the Kafka topic.
	if err := kafkaConnector.ProduceMessages(*activityTopic, messages); err != nil {
		logs.Logger.Error("error:", err)
		return err
	}

	// Close the KafkaConnector when done.
	if err := kafkaConnector.Close(); err != nil {
		logs.Logger.Error("error:", err)
		return err
	}
	fmt.Println("Message successfully produced to Kafka.")
	return nil
}

func RunKafkaConsumerActivity() error {

	kafkaConnector, _, activityTopic, err := service.ConfigureKafka()
	if err != nil {
		logs.Logger.Error("error:", err)
		return err
	}
	// Consume messages from the Kafka topic.
	if err := kafkaConnector.ConsumeMessages(*activityTopic); err != nil {
		logs.Logger.Error("error:", err)
		return err
	}

	// Close the KafkaConnector when done (this will only be reached after receiving a signal to stop).
	if err := kafkaConnector.Close(); err != nil {
		logs.Logger.Error("error:", err)
		return err
	}

	return nil
}
