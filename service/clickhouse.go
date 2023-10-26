package service

import (
	"database/sql"
	"datastream/config"
	"datastream/logs"
	"datastream/types"

	"fmt"

	_ "github.com/ClickHouse/clickhouse-go"
)

type ClickHouseConnector struct {
	config config.ClickHouseConfig
	db     *sql.DB
}

func (c *ClickHouseConnector) Connect() (*sql.DB, error) {
	if c.db != nil {
		return c.db, nil
	}

	dataSourceName := fmt.Sprintf("tcp://%s:%s?database=%s&username=%s&password=%s",
		c.config.Hostname,
		c.config.Port,
		c.config.DBName,
		c.config.Username,
		c.config.Password)
	fmt.Println(dataSourceName)
	db, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {
		logs.Logger.Error("failed to open ClickHouse connection:", err)
		return nil, fmt.Errorf("failed to open ClickHouse connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		logs.Logger.Error("Error:", err)
		return nil, err
	}

	c.db = db
	return db, nil
}

func (c *ClickHouseConnector) Close() error {
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		logs.Logger.Error("Error:", err)
		return err
	}
	return nil
}

func ConfigureClickHouseDB(configmsg string) (*ClickHouseConnector, error) {
	configData, err := config.LoadDatabaseConfig(configmsg)
	if err != nil {
		logs.Logger.Error("failed to load database config", err)
		return nil, fmt.Errorf("failed to load database config: %v", err)
	}
	clickHouseConfig, ok := configData.(config.ClickHouseConfig)
	if !ok {
		logs.Logger.Error("expected ClickHouseConfig, but got", err)
		return nil, fmt.Errorf("expected ClickHouseConfig, but got %T", configData)
	}

	clickHouseConnector := ClickHouseConnector{config: clickHouseConfig}

	return &clickHouseConnector, nil
}

func QueryTopContactActivity(query string) ([]types.QueryOutput, error) {

	dbConnector, err := ConfigureClickHouseDB("clickhouse")
	if err != nil {
		logs.Logger.Error("failed to configure ClickHouse DB:", err)
		return nil, fmt.Errorf("failed to configure ClickHouse DB: %v", err)
	}

	db, err := dbConnector.Connect()
	if err != nil {
		logs.Logger.Error("error connecting to the database:", err)
		return nil, fmt.Errorf("error connecting to the database: %v", err)
	}
	defer db.Close()
	logs.Logger.Info("Database connection is available")
	rows, err := db.Query(query)
	if err != nil {
		logs.Logger.Error("error executing query:", err)
		return nil, fmt.Errorf("error executing query: %v", err)
	}
	defer rows.Close()

	var results []types.QueryOutput
	for rows.Next() {

		var row types.QueryOutput

		err := rows.Scan(&row.ContactID, &row.Click)

		if err != nil {
			logs.Logger.Error("error in rowscan:", err)
			return nil, fmt.Errorf("error in rowscan: %v", err)

		}

		results = append(results, row)

	}

	return results, nil
}
