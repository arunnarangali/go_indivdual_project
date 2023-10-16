package service

import (
	"datastream/config"
	"datastream/logs"
	"fmt"
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

// // // ProduceMessage produces a message into the specified Kafka topic.
// func (kc *KafkaConnector) ProduceMessages(topic string, messageValues []string) error {
// 	count := 0
// 	var msg []string

// 	for _, messageValue := range messageValues {
// 		msg = append(msg, messageValue)
// 		count++
// 		if count == 50 {
// 			msgToProduce := &sarama.ProducerMessage{
// 				Topic: topic,
// 				Value: sarama.StringEncoder(strings.Join(msg, "\n")),
// 			}
// 			_, _, err := kc.Producer.SendMessage(msgToProduce)
// 			if err != nil {
// 				logs.Logger.Error("error:", err)
// 				return err
// 			}
// 			count = 0
// 			msg = []string{}
// 		}
// 	}
// 	if count > 0 {
// 		msgToProduce := &sarama.ProducerMessage{
// 			Topic: topic,
// 			Value: sarama.StringEncoder(strings.Join(msg, "\n")),
// 		}

// 		_, _, err := kc.Producer.SendMessage(msgToProduce)
// 		if err != nil {
// 			logs.Logger.Error("error:", err)
// 			return err
// 		}
// 	}

// 	return nil
// }

// // ProduceMessage produces a message into the specified Kafka topic.
func (kc *KafkaConnector) ProduceMessages(topic string, messageValues []string) error {
	msgToProduce := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(strings.Join(messageValues, "\n")),
	}

	partition, offset, err := kc.Producer.SendMessage(msgToProduce)
	if err != nil {
		logs.Logger.Error("error:", err)
		return err
	}

	logs.Logger.Info(fmt.Sprintf("Topic: %s Message produced to partition: %d at offset: %d \n", topic, partition, offset))

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

	for {
		select {
		case err := <-partitionConsumer.Errors():
			log.Printf("Error in Kafka consumer: %v", err)
			// Handle the error as needed.
		case msg := <-partitionConsumer.Messages():

			log.Printf("Received message from topic %s, partition %d, offset %d: %s",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
			logs.Logger.Info(fmt.Sprintf("Received message from topic %s, partition %d, offset %d: %d",
				msg.Topic, msg.Partition, msg.Offset, len(msg.Value)))
			message := string(msg.Value)
			msgStrings := []string{string(msg.Value)}

			if !strings.Contains(message, "Eof") {
				err := Insertmsg(msgStrings, topic)
				if err != nil {
					log.Printf("Error inserting contact: %v", err)

				} else {
					logs.Logger.Info("inserted Successfully")
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

func RunKafkaProducerContacts(messages []string) error {
	var msg []string
	count := 0
	kafkaConnector, contactTopic, _, err := ConfigureKafka()
	if err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}
	// Produce a message to the Kafka topic.
	for _, messageValue := range messages {
		msg = append(msg, messageValue)
		count++
		if count == 1000 {
			if err := kafkaConnector.ProduceMessages(*contactTopic, msg); err != nil {
				logs.Logger.Error("Error:", err)
				return err
			}
			count = 0
			msg = []string{}
		}
	}

	if count > 0 {
		if err := kafkaConnector.ProduceMessages(*contactTopic, msg); err != nil {
			logs.Logger.Error("Error:", err)
			return err
		}
	}
	// Close the KafkaConnector when done.
	if err := kafkaConnector.Close(); err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}
	logs.Logger.Info("Messages successfully send contact to kafka")
	fmt.Println("Messages successfully produced to Kafka.")
	return nil
}

func RunKafkaConsumerContacts() error {

	kafkaConnector, contactTopic, _, err := ConfigureKafka()
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
	var msg []string
	count := 0
	kafkaConnector, _, activityTopic, err := ConfigureKafka()
	if err != nil {
		logs.Logger.Error("error:", err)
		return err
	}

	// Produce a message to the Kafka topic.
	for _, messageValue := range messages {
		msg = append(msg, messageValue)
		count++
		if count == 50 {
			if err := kafkaConnector.ProduceMessages(*activityTopic, msg); err != nil {
				logs.Logger.Error("Error:", err)
				return err
			}
			count = 0
			msg = []string{}
		}
	}

	if count > 0 {
		if err := kafkaConnector.ProduceMessages(*activityTopic, msg); err != nil {
			logs.Logger.Error("Error:", err)
			return err
		}
	}
	// Close the KafkaConnector when done.
	if err := kafkaConnector.Close(); err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}
	logs.Logger.Info("Message successfully send activity to Kafka")
	fmt.Println("Message successfully produced to  activity Kafka.")
	return nil
}

func RunKafkaConsumerActivity() error {

	kafkaConnector, _, activityTopic, err := ConfigureKafka()
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
