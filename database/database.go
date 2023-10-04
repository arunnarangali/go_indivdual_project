package database

import (
	"datastream/config"
	"fmt"

	"github.com/IBM/sarama"
)

type Database interface {
	Query(query string) ([]map[string]interface{}, error)
	Exec(query string) error
}

func ProduceKafkaMessageActivity(msg string) error {
	// Load Kafka configuration from the .env file.
	kafkaConfig, err := config.LoadDatabaseConfig("kafka")
	if err != nil {
		return fmt.Errorf("error loading Kafka configuration: %v", err)
	}

	// Create a KafkaConnector instance.
	kafkaConnector, err := config.NewKafkaConnector(kafkaConfig.(config.KafkaConfig))
	if err != nil {
		return fmt.Errorf("error creating Kafka connector: %v", err)
	}
	defer kafkaConnector.Close() // Close Kafka connections when done.

	// Now you can use kafkaConnector to produce messages.

	topic := kafkaConfig.(config.KafkaConfig).Topic1

	// Produce a message to the Kafka topic.
	_, _, err = kafkaConnector.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	})

	if err != nil {
		return fmt.Errorf("error producing message: %v", err)
	}

	fmt.Printf("Produced message: %s\n", msg)
	return nil
}

func ProduceKafkaMessageContacts(msg string) error {
	// Load Kafka configuration from the .env file.
	kafkaConfig, err := config.LoadDatabaseConfig("kafka")
	if err != nil {
		return fmt.Errorf("error loading Kafka configuration: %v", err)
	}

	// Create a KafkaConnector instance.
	kafkaConnector, err := config.NewKafkaConnector(kafkaConfig.(config.KafkaConfig))
	if err != nil {
		return fmt.Errorf("error creating Kafka connector: %v", err)
	}
	defer kafkaConnector.Close() // Close Kafka connections when done.

	// Now you can use kafkaConnector to produce messages.

	topic := kafkaConfig.(config.KafkaConfig).Topic2

	// Produce a message to the Kafka topic.
	_, _, err = kafkaConnector.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	})

	if err != nil {
		return fmt.Errorf("error producing message: %v", err)
	}

	fmt.Printf("Produced message: %s\n", msg)
	return nil
}
