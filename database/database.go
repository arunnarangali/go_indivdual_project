package database

import (
	"datastream/config"
	"datastream/logs"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"

	_ "github.com/go-sql-driver/mysql"
)

type Database interface {
	Query(query string) ([]map[string]interface{}, error)
	Exec(query string) error
}

func ProduceKafkaMessageActivity(msg string) error {
	log1 := logs.Createlogfile()
	// Load Kafka configuration from the .env file.
	kafkaConfig, err := config.LoadDatabaseConfig("kafka")
	if err != nil {
		log1.Error(err.Error())
		return fmt.Errorf("error loading Kafka configuration: %v", err)
	}

	// Create a KafkaConnector instance.
	kafkaConnector, err := config.NewKafkaConnector(kafkaConfig.(config.KafkaConfig))
	if err != nil {
		log1.Error(err.Error())
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
		log1.Error(err.Error())
		log1.Warning("error producing message")
		return fmt.Errorf("error producing message: %v", err)
	}

	actmsges := fmt.Sprintf("Produced message: %s\n", msg)
	log1.Info(actmsges)
	return nil
}

func ProduceKafkaMessageContacts(msg string) error {
	log1 := logs.Createlogfile()
	// Load Kafka configuration from the .env file.
	kafkaConfig, err := config.LoadDatabaseConfig("kafka")
	if err != nil {
		log1.Error(err.Error())
		return fmt.Errorf("error loading Kafka configuration: %v", err)
	}

	// Create a KafkaConnector instance.
	kafkaConnector, err := config.NewKafkaConnector(kafkaConfig.(config.KafkaConfig))
	if err != nil {
		log1.Error(err.Error())
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

		log1.Error(err.Error())
		return fmt.Errorf("error producing message: %v", err)
	}

	fmt.Printf("Produced message: %s\n", msg)
	return nil
}

func ConfigureMySQLDB() (*config.MySQLConnector, error) {
	log1 := logs.Createlogfile()
	// Define the MySQL database type.
	dbType := "mysql"

	// Load the MySQL configuration.
	configData, err := config.LoadDatabaseConfig(dbType)
	if err != nil {
		log1.Error(err.Error())
		return nil, fmt.Errorf("failed to load database config: %v", err)
	}

	// Ensure the database type is MySQL.
	mysqlConfig, ok := configData.(config.MySQLConfig)
	if !ok {
		log1.Error(err.Error())
		return nil, fmt.Errorf("expected MySQLConfig, but got %T", configData)
	}

	// Create a MySQLConnector instance.
	mysqlConnector := config.MySQLConnector{Config: mysqlConfig}

	return &mysqlConnector, nil
}

func ExecuteInsertQuery(mysqlConnector *config.MySQLConnector, messages []string) error {
	log1 := logs.Createlogfile()
	// Connect to the MySQL database.
	db, err := mysqlConnector.Connect()
	if err != nil {
		log1.Error(err.Error())
		return fmt.Errorf("failed to connect to MySQL: %v", err)
	}
	defer mysqlConnector.Close()

	// Define the INSERT query with placeholders.
	query := `
      INSERT INTO ContactActivity (ContactsID,CampaignID,ActivityType,ActivityDate)
      VALUES (?, ?, ?, ?)
    `

	// Start a transaction.
	tx, err := db.Begin()
	if err != nil {
		log1.Error(err.Error())
		return fmt.Errorf("failed to start a transaction: %v", err)
	}

	// Prepare the INSERT statement.
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		log1.Error(err.Error())
		return fmt.Errorf("failed to prepare INSERT statement: %v", err)
	}

	for _, message := range messages {
		values := strings.Split(message, ",")
		if len(values) >= 4 {
			contactsid, err := strconv.Atoi(values[0])
			if err != nil {
				tx.Rollback()
				fmt.Printf("Invalid contactsid: %v\n", values[0]) // Log the invalid contactsid.
				return fmt.Errorf("invalid contactsid: %v", values[0])
			}

			campaignidStr := strings.TrimSpace(values[1])
			campaignid, err := strconv.Atoi(campaignidStr)
			if err != nil {
				tx.Rollback()
				log1.Warning(fmt.Sprintf("Invalid campaignid: %v\n", values[1])) // Log the invalid campaignid.
				return fmt.Errorf("invalid campaignid: %v", values[1])
			}
			activitytypeStr := strings.TrimSpace(values[2])
			activitytype, err := strconv.Atoi(activitytypeStr)
			if err != nil {
				tx.Rollback()
				log1.Error(fmt.Sprintf("Invalid activitytype: %v\n", values[2])) // Log the invalid activitytype.
				return fmt.Errorf("invalid activitytype: %v", values[2])
			}
			activitydateStr := strings.TrimSpace(values[3])

			// Remove double quotes from activitydateStr.
			activitydateStr = strings.Trim(activitydateStr, `"`)

			// Parse 'activitydate' string to a time.Time object.
			activitydate, err := time.Parse("2006-01-02", activitydateStr)
			if err != nil {
				tx.Rollback()
				log1.Error(fmt.Sprintf("Invalid activitydate: %v\n", activitydateStr)) // Log the invalid activitydate.
				return fmt.Errorf("invalid activitydate: %v", err)
			}

			_, err = stmt.Exec(contactsid, campaignid, activitytype, activitydate)
			if err != nil {
				tx.Rollback()
				log1.Error(fmt.Sprintf("Failed to execute INSERT query: %v\n", err)) // Log the INSERT query failure.
				return fmt.Errorf("failed to execute INSERT query: %v", err)
			}
		} else {
			tx.Rollback()
			log1.Error(fmt.Sprintf("Invalid message format: %v\n", message)) // Log the invalid message.
			return fmt.Errorf("invalid message format: %v", message)
		}

	}

	// Commit the transaction.
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		log1.Error(fmt.Sprintf("failed to commit transaction: %v", err))
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	log1.Info(fmt.Sprintf("Inserted %d messages into MySQL.\n", len(messages)))
	return nil
}

func ConsumeKafkaMessages(mysqlConnector *config.MySQLConnector) error {
	log1 := logs.Createlogfile()
	// Load Kafka configuration from the .env file.
	kafkaConfig, err := config.LoadDatabaseConfig("kafka")
	if err != nil {
		log1.Error(fmt.Sprintf("Error loading Kafka configuration: %v\n", err))
		return err
	}

	// Create a KafkaConnector instance.
	kafkaConnector, err := config.NewKafkaConnector(kafkaConfig.(config.KafkaConfig))
	if err != nil {
		log1.Error(fmt.Sprintf("Error creating Kafka connector: %v\n", err))
		return err
	}
	defer kafkaConnector.Close() // Close Kafka connections when done.

	topic := kafkaConfig.(config.KafkaConfig).Topic1

	// Create a Kafka consumer for the specified topic, starting from the oldest offset.
	consumer, err := kafkaConnector.Consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log1.Error(fmt.Sprintf("Error creating Kafka consumer: %v\n", err))
		return err
	}

	fmt.Printf("Kafka consumer listening on topic: %s\n", topic)
	log1.Info(fmt.Sprintf("Kafka consumer listening on topic: %s\n", topic))

	// Create a buffer to accumulate messages.
	batchSize := 100
	messageBuffer := make([]string, 0, batchSize)

	// Start a loop to continuously consume messages from the channel.
	for msg := range consumer.Messages() {
		// Process the Kafka message (you can perform any desired action here).
		fmt.Printf("Received Kafka message: %s\n", string(msg.Value))
		log1.Info(fmt.Sprintf("Received Kafka message: %s\n", string(msg.Value)))

		message := string(msg.Value)

		// Split the message into individual lines (assuming each line is a separate message).
		lines := strings.Split(strings.TrimSpace(message), "\n")

		// Check if the message contains "Eof" anywhere in it.
		if !strings.Contains(message, "Eof") {
			// Accumulate messages in the buffer.
			messageBuffer = append(messageBuffer, lines...)
		} else {
			return nil
		}

		// If the buffer has reached the desired batch size, insert into MySQL and reset the buffer.
		if len(messageBuffer) >= batchSize {
			if err := ExecuteInsertQuery(mysqlConnector, messageBuffer); err != nil {
				log1.Error(err.Error())
				fmt.Printf("Error inserting batch into MySQL: %v\n", err)
				// Add more detailed logging here, if needed.
			} else {
				log1.Info(fmt.Sprintln("Inserted batch into MySQL."))

				fmt.Println("Inserted batch into MySQL.")
			}
			// Clear the buffer.
			messageBuffer = messageBuffer[:0]
		}
	}

	return nil
}

func ConsumeKafkaMessagesContact(mysqlConnector *config.MySQLConnector) error {
	log1 := logs.Createlogfile()
	// Load Kafka configuration from the .env file.
	kafkaConfig, err := config.LoadDatabaseConfig("kafka")
	if err != nil {
		log1.Error(err.Error())
		fmt.Printf("Error loading Kafka configuration: %v\n", err)
		return err
	}

	// Create a KafkaConnector instance.
	kafkaConnector, err := config.NewKafkaConnector(kafkaConfig.(config.KafkaConfig))
	if err != nil {
		log1.Error(err.Error())
		fmt.Printf("Error creating Kafka connector: %v\n", err)
		return err
	}
	defer kafkaConnector.Close() // Close Kafka connections when done.

	topic := kafkaConfig.(config.KafkaConfig).Topic2

	// Create a Kafka consumer for the specified topic, starting from the oldest offset.
	consumer, err := kafkaConnector.Consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log1.Error(err.Error())
		fmt.Printf("Error creating Kafka consumer: %v\n", err)
		return err
	}

	log1.Info(fmt.Sprintf("Kafka consumer listening on topic: %s\n", topic))

	fmt.Printf("Kafka consumer listening on topic: %s \n", topic)

	// Create a buffer to accumulate messages.
	batchSize := 1000
	messageBuffer := make([]string, 0, batchSize)

	// Start a loop to continuously consume messages from the channel.
	for msg := range consumer.Messages() {
		// Process the Kafka message (you can perform any desired action here).
		fmt.Printf("Received Kafka message: %s\n", string(msg.Value))
		log1.Info(fmt.Sprintf("Received Kafka message: %s\n", string(msg.Value)))
		message := string(msg.Value)

		// Split the message into individual lines (assuming each line is a separate message).
		lines := strings.Split(strings.TrimSpace(message), "\n")
		// Check if the message contains "Eof" anywhere in it.
		if !strings.Contains(message, "Eof") {
			// Accumulate messages in the buffer.
			messageBuffer = append(messageBuffer, lines...)
		} else {
			return nil
		}
		// If the buffer has reached the desired batch size, insert into MySQL and reset the buffer.
		if len(messageBuffer) >= batchSize {
			if err := ExecuteInsertQueryContacts(mysqlConnector, messageBuffer); err != nil {
				log1.Error(fmt.Sprintf("Error inserting batch into MySQL: %v\n", err))
				fmt.Printf("Error inserting batch into MySQL: %v\n", err)
				// Add more detailed logging here, if needed.
			} else {
				fmt.Println("Inserted batch into MySQL.")
				log1.Info(fmt.Sprintln("Inserted batch into MySQL."))

			}
			// Clear the buffer.
			messageBuffer = messageBuffer[:0]
		}
	}

	return nil
}
func ExecuteInsertQueryContacts(mysqlConnector *config.MySQLConnector, messages []string) error {
	log1 := logs.Createlogfile()
	// Connect to the MySQL database.
	db, err := mysqlConnector.Connect()
	if err != nil {
		log1.Error(err.Error())
		return fmt.Errorf("failed to connect to MySQL: %v", err)
	}
	defer mysqlConnector.Close()

	// Define the INSERT query for the 'contacts' table with placeholders.
	query := `
      INSERT INTO Contacts (Name, Email, Details, Status)
      VALUES (?, ?, ?, ?)
    `

	// Start a transaction.
	tx, err := db.Begin()
	if err != nil {
		log1.Error(err.Error())
		return fmt.Errorf("failed to start a transaction: %v", err)
	}

	// Prepare the INSERT statement.
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		log1.Error(fmt.Sprintf("failed to prepare INSERT statement: %v", err))
		return fmt.Errorf("failed to prepare INSERT statement: %v", err)
	}
	for _, message := range messages {

		// Trim leading and trailing spaces from the message.
		message = strings.TrimSpace(message)
		// Split the message using commas and spaces as delimiters.
		// values := strings.Split(strings.TrimSpace(message), ",")
		values := strings.SplitN(string(message), ",", 4)

		if len(values) >= 4 {
			name := strings.TrimSpace(values[0])
			email := strings.TrimSpace(values[1])
			detailsStr := strings.TrimSpace(values[3])
			statusStr := strings.TrimSpace(values[2])

			fmt.Printf("Processing message: name=%s, email=%s, details=%s, status=%s\n", name, email, detailsStr, statusStr)
			log1.Info(fmt.Sprintf("Processing message: name=%s, email=%s, details=%s, status=%s\n", name, email, detailsStr, statusStr))

			// Convert statusStr to an integer.
			status, err := strconv.Atoi(statusStr)
			if err != nil {
				tx.Rollback()
				log1.Error(fmt.Sprintf("Invalid status format: %v\n", statusStr)) // Log the invalid status.
				return fmt.Errorf("invalid status format: %v", err)
			}
			_, err = stmt.Exec(name, email, detailsStr, status)
			if err != nil {
				tx.Rollback()
				log1.Error(fmt.Sprintf("Failed to execute INSERT query: %v\n", err)) // Log the INSERT query failure.
				return fmt.Errorf("failed to execute INSERT query: %v", err)
			}
		} else {
			tx.Rollback()
			log1.Error(fmt.Sprintf("Invalid message format: %v\n", message)) // Log the invalid message.
			return fmt.Errorf("invalid message format: %v", message)
		}
	}

	// Commit the transaction.
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		log1.Error(err.Error())
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	fmt.Printf("Inserted %d messages into 'contacts' table.\n", len(messages))
	log1.Info(fmt.Sprintf("Inserted %d messages into 'contacts' table.\n", len(messages)))

	return nil
}
