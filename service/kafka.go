package service

import (
	"datastream/config"
	"log"
	"strings"

	"github.com/IBM/sarama"
)

// KafkaConnector implements the DBConnector interface for Kafka using IBM's sarama package.
type KafkaConnector struct {
	Config   config.KafkaConfig
	Producer sarama.SyncProducer
	Consumer sarama.Consumer
}

// NewKafkaConnector creates a new KafkaConnector instance.
func NewKafkaConnector(kafkaConfig config.KafkaConfig) (*KafkaConnector, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true

	broker := kafkaConfig.Broker

	consumerGroup, err := sarama.NewConsumer([]string{broker}, nil)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		return nil, err
	}

	return &KafkaConnector{
		Config:   kafkaConfig,
		Producer: producer,
		Consumer: consumerGroup,
	}, nil
}

// Close closes the Kafka producer and consumer.
func (kc *KafkaConnector) Close() error {
	if kc.Producer != nil {
		return kc.Producer.Close()
	}
	if kc.Consumer != nil {
		return kc.Consumer.Close()
	}
	return nil
}

// ProduceMessage produces a message into the specified Kafka topic.
func (kc *KafkaConnector) ProduceMessage(topic string, messageValue string) error {
	// Create a new message to send to Kafka.
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(messageValue),
	}

	// Send the message to the Kafka topic using the producer.
	_, _, err := kc.Producer.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}
func (kc *KafkaConnector) ConsumeMessages(topic string) error {
	// Create a new consumer for the specified topic and partition.
	partitionConsumer, err := kc.Consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	defer partitionConsumer.Close()

	// Start a goroutine to handle consumed messages.
	for {
		select {
		case err := <-partitionConsumer.Errors():
			log.Printf("Error in Kafka consumer: %v", err)
			// Handle the error as needed.
		case msg := <-partitionConsumer.Messages():
			// Process the consumed message here.
			log.Printf("Received message from topic %s, partition %d: %s",
				msg.Topic, msg.Partition, string(msg.Value))
			message := string(msg.Value)
			msgStrings := []string{string(msg.Value)}

			// Check if the message contains "Eof" anywhere in it.
			if !strings.Contains(message, "Eof") {
				// Assuming you have a dbConnector instance
				err := Getmsg(msgStrings, topic)
				if err != nil {
					log.Printf("Error inserting contact: %v", err)
					// Handle the error as needed, possibly continue processing.
				} else {
					log.Printf("Contact inserted successfully ")
				}
			} else {
				return nil // Return nil when the message contains "Eof."
			}
		}
	}
}

func ConfigureKafka() (*KafkaConnector, *string, *string, error) {
	// Load Kafka configuration from your environment variables or config file.
	kafkaConfig, err := config.LoadDatabaseConfig("kafka")
	if err != nil {
		return nil, nil, nil, err
	}

	// Create a KafkaConnector instance.
	kafkaConnector, err := NewKafkaConnector(kafkaConfig.(config.KafkaConfig))
	if err != nil {
		return nil, nil, nil, err
	}
	// Specify the topic and message value you want to produce.
	contactTopic := kafkaConfig.(config.KafkaConfig).Topic2
	activityTopic := kafkaConfig.(config.KafkaConfig).Topic1
	return kafkaConnector, &contactTopic, &activityTopic, nil
}
