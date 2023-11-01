package Process

import (
	"datastream/logs"
	"datastream/service"
	"fmt"
	"log"
)

func RunKafkaProducerContacts(messages []string) error {

	kafkaConnector, contactTopic, _, err := service.ConfigureKafka("kafka")
	if err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}

	if len(messages) == 0 {
		logs.Logger.Error("Error", fmt.Errorf("message string is empty"))
		return fmt.Errorf("message string is empty")
	}

	if err := kafkaConnector.ProduceMessages(*contactTopic, messages); err != nil {
		logs.Logger.Error("Error:", err)
		return err
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

	kafkaConnector, contactTopic, _, err := service.ConfigureKafka("kafka")
	if err != nil {
		logs.Logger.Error("error in configure:", err)
		return err
	}

	ContactsMSg, err := kafkaConnector.ConsumeMessages(*contactTopic)
	if err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}

	err = Insertmsg(ContactsMSg, *contactTopic)

	if err != nil {
		log.Printf("Error inserting Contacts: %v", err)
		logs.Logger.Error("Error inserting contacts:", err)

	} else {
		logs.Logger.Info("inserted  contact Successfully")
		log.Printf(" inserted  contact successfully ")
	}

	if err := kafkaConnector.Close(); err != nil {
		logs.Logger.Error("err in kafkaclose", err)
		return err
	}
	return nil
}

func RunKafkaProducerActivity(messages []string) error {

	kafkaConnector, _, activityTopic, err := service.ConfigureKafka("kafka")
	if err != nil {
		logs.Logger.Error("error:", err)
		return err
	}

	if len(messages) == 0 {
		logs.Logger.Error("Error", fmt.Errorf("message string is empty"))
		return fmt.Errorf("message string is empty")
	}

	if err := kafkaConnector.ProduceMessages(*activityTopic, messages); err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}

	if err := kafkaConnector.Close(); err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}

	logs.Logger.Info("Message successfully send activity to Kafka")

	fmt.Println("Message successfully produced to  activity Kafka.")
	return nil
}

func RunKafkaConsumerActivity() error {

	kafkaConnectoractivity, _, activityTopic, err := service.ConfigureKafka("kafka")
	if err != nil {
		logs.Logger.Error("error:", err)
		return err
	}

	activitymsg, err := kafkaConnectoractivity.ConsumeMessages(*activityTopic)
	if err != nil {
		logs.Logger.Error("Error:", err)
		return err
	}

	err = Insertmsg(activitymsg, *activityTopic)
	if err != nil {
		log.Printf("Error inserting activity: %v", err)
		logs.Logger.Error("Error inserting activity:", err)

	} else {
		logs.Logger.Info("inserted  Activity Successfully")
		log.Printf(" inserted  Activity successfully ")
	}

	if err := kafkaConnectoractivity.Close(); err != nil {
		logs.Logger.Error("error:", err)
		return err
	}

	return nil
}
