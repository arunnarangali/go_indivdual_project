package service

import (
	"database/sql"

	"datastream/Process"
	"datastream/config"
	"datastream/logs"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type DBConnector interface {
	Connect() (*sql.DB, error)
	Close() error
}

type MySQLConnector struct {
	Config    config.MySQLConfig
	Db        *sql.DB
	Connected bool
}

func (m *MySQLConnector) Connect() (*sql.DB, error) {
	if m.Db != nil {
		return m.Db, nil
	}

	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		m.Config.Username,
		m.Config.Password,
		m.Config.Hostname,
		m.Config.Port,
		m.Config.DBName)

	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		logs.Logger.Error("error:", err)
		return nil, err
	}

	if err := db.Ping(); err != nil {
		logs.Logger.Error("error:", err)
		return nil, err
	}

	m.Db = db
	return db, nil
}

func (m *MySQLConnector) Close() error {
	if m.Db != nil {
		err := m.Db.Close()
		m.Db = nil
		logs.Logger.Error("error:", err)
		return err
	}
	return nil
}

func ConfigureMySQLDB(configmsg string) (*MySQLConnector, error) {

	if len(configmsg) <= 0 {
		logs.Logger.Error("error:", fmt.Errorf("configmsg is nil"))
		return nil, fmt.Errorf("configmsg is nil")
	}
	configData, err := config.LoadDatabaseConfig(configmsg)
	if err != nil {
		logs.Logger.Error("error:", err)
		return nil, fmt.Errorf("failed to load database config: %v", err)
	}

	mysqlConfig, ok := configData.(config.MySQLConfig)

	if !ok {
		return nil, fmt.Errorf("expected MySQLConfig, but got %T", configData)
	}

	mysqlConnector := MySQLConnector{Config: mysqlConfig}

	return &mysqlConnector, nil
}

func Insertmsg(msg []string, topic string) error {

	dbConnector, err := ConfigureMySQLDB("mysql")
	if err != nil {
		logs.Logger.Error("error to get config", err)
		return fmt.Errorf("error to get config: %v", err)
	}

	// Connect to the database
	db, err := dbConnector.Connect()
	if err != nil {
		logs.Logger.Error("error connecting to the database:", err)
		return fmt.Errorf("error connecting to the database: %v", err)
	}

	defer db.Close()

	err = Process.HandleTopic(db, msg, topic)

	if err != nil {
		logs.Logger.Error("error in handle topic:", err)
		return fmt.Errorf("error in handle topic: %v", err)
	}

	return nil
}
