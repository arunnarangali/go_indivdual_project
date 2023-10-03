package database

type Database interface {
	Query(query string) ([]map[string]interface{}, error)
	Exec(query string) error
}
