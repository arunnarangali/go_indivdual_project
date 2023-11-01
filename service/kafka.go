package service

import (
	"datastream/config"
	"datastream/logs"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/IBM/sarama"
)

// var kafkaConnector, contactTopic, activityTopic, err = ConfigureKafka("kafka")

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

func (kc *KafkaConnector) ProduceMessages(topic string, messageValues []string) error {
	if len(messageValues) > 0 {
		msgToProduce := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(strings.Join(messageValues, "\n")),
		}

		partition, offset, err := kc.Producer.SendMessage(msgToProduce)
		if err != nil {
			logs.Logger.Error("error:", err)
			return err
		}

		logs.Logger.Info(fmt.Sprintf("Producer Topic: %s Message produced to partition: %d at offset: %d \n",
			topic, partition, offset))

		return nil
	} else {
		logs.Logger.Error("Erorr", errors.New("messageValues is empty"))
		return errors.New("messageValues is empty")
	}
}

func (kc *KafkaConnector) ConsumeMessages(topic string) ([]string, error) {

	partitionConsumer, err := kc.Consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		logs.Logger.Error("error:", err)
		return nil, err
	}
	defer partitionConsumer.Close()
	for {
		select {
		case err := <-partitionConsumer.Errors():
			log.Printf("Error in Kafka consumer: %v", err)

		case msg := <-partitionConsumer.Messages():

			log.Printf("Received message from topic %s, partition %d, offset %d: %s",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Value))

			logs.Logger.Info(fmt.Sprintf("Received message from topic %s, partition %d, offset %d: %d",
				msg.Topic, msg.Partition, msg.Offset, len(msg.Value)))

			message := string(msg.Value)
			msgStrings := []string{message}

			return msgStrings, err
		}
	}
}

func ConfigureKafka(configMsg string) (*KafkaConnector, *string, *string, error) {
	if len(configMsg) <= 0 {
		logs.Logger.Error("error:", fmt.Errorf("configmsg is nil"))
		return nil, nil, nil, fmt.Errorf("configmsg is nil")
	}
	kafkaConfig, err := config.LoadDatabaseConfig(configMsg)
	if err != nil {
		logs.Logger.Error("error:", err)
		return nil, nil, nil, err
	}

	kafkaConnector, err := NewKafkaConnector(kafkaConfig.(config.KafkaConfig))
	if err != nil {
		logs.Logger.Error("error:", err)
		return nil, nil, nil, err
	}

	contactTopic := kafkaConfig.(config.KafkaConfig).Topic2
	activityTopic := kafkaConfig.(config.KafkaConfig).Topic1
	return kafkaConnector, &contactTopic, &activityTopic, nil
}
