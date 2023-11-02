package service

import (
	"database/sql"
	"strings"

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

func InsertDataToMySql(db *sql.DB, tablename string,
	columnNames []string, dataSlice [][]interface{}) ([]int64, error) {
	tx, err := db.Begin()
	if err != nil {
		logs.Logger.Error("error starting a transaction:", err)
		return nil, fmt.Errorf("error starting a transaction: %v", err)
	}
	defer tx.Rollback()

	ids := []int64{}

	query := "INSERT INTO " + tablename + " (" + strings.Join(columnNames, ", ") + ") VALUES ("
	query += strings.Repeat("?, ", len(columnNames)-1) + "?)"

	stmt, err := tx.Prepare(query)
	if err != nil {
		logs.Logger.Error("error preparing statement:", err)
		return nil, fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	for _, rowData := range dataSlice {
		res, err := stmt.Exec(rowData...)
		if err != nil {
			logs.Logger.Error("error inserting data:", err)
			return nil, fmt.Errorf("error inserting data: %v", err)
		}

		lastInsertID, err := res.LastInsertId()
		if err != nil {
			logs.Logger.Error("error:", err)
			return nil, fmt.Errorf("error getting last insert ID: %v", err)
		}
		ids = append(ids, lastInsertID)
	}

	if err := tx.Commit(); err != nil {
		logs.Logger.Error("error committing transaction:", err)
		return nil, fmt.Errorf("error committing transaction: %v", err)
	}

	return ids, nil
}
