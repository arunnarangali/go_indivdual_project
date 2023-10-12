package service

import (
	"datastream/config"
	"datastream/logs"
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
		logs.Logger.Error("error:", err)
		return nil, err
	}

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		logs.Logger.Error("error:", err)
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

// // ProduceMessage produces a message into the specified Kafka topic.
func (kc *KafkaConnector) ProduceMessages(topic string, messageValues []string) error {
	count := 0
	var msg []string

	for _, messageValue := range messageValues {
		msg = append(msg, messageValue)
		count++
		if count == 50 {
			msgToProduce := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(strings.Join(msg, "\n")),
			}
			_, _, err := kc.Producer.SendMessage(msgToProduce)
			if err != nil {
				logs.Logger.Error("error:", err)
				return err
			}
			count = 0
			msg = []string{}
		}
	}
	if count > 0 {
		msgToProduce := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(strings.Join(msg, "\n")),
		}

		_, _, err := kc.Producer.SendMessage(msgToProduce)
		if err != nil {
			logs.Logger.Error("error:", err)
			return err
		}
	}

	return nil
}

func (kc *KafkaConnector) ConsumeMessages(topic string) error {
	// Create a new consumer for the specified topic and partition.
	partitionConsumer, err := kc.Consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		logs.Logger.Error("error:", err)
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

			log.Printf("Received message from topic %s, partition %d: %s",
				msg.Topic, msg.Partition, string(msg.Value))
			message := string(msg.Value)
			msgStrings := []string{string(msg.Value)}

			if !strings.Contains(message, "Eof") {
				err := Getmsg(msgStrings, topic)
				if err != nil {
					log.Printf("Error inserting contact: %v", err)

				} else {
					log.Printf(" inserted successfully ")
				}
			} else {
				return nil
			}
		}
	}
}

func ConfigureKafka() (*KafkaConnector, *string, *string, error) {

	kafkaConfig, err := config.LoadDatabaseConfig("kafka")
	if err != nil {
		logs.Logger.Error("error:", err)
		return nil, nil, nil, err
	}

	// Create a KafkaConnector instance.
	kafkaConnector, err := NewKafkaConnector(kafkaConfig.(config.KafkaConfig))
	if err != nil {
		logs.Logger.Error("error:", err)
		return nil, nil, nil, err
	}
	// Specify the topic and message value you want to produce.
	contactTopic := kafkaConfig.(config.KafkaConfig).Topic2
	activityTopic := kafkaConfig.(config.KafkaConfig).Topic1
	return kafkaConnector, &contactTopic, &activityTopic, nil
}
