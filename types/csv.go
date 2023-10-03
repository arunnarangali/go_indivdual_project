package types

import (
	"datastream/logs"
	"encoding/csv"
	"fmt"
	"io"
)

// type CSVData struct {
// 	Id    int
// 	Name  string
// 	Email string
// 	Json  string
// }

type CSVReader interface {
	ReadCSV(reader io.Reader) ([]Contacts, error)
}

type MyCSVReader struct{}

func (r MyCSVReader) ReadCSV(reader io.Reader) ([]Contacts, error) {
	log1 := logs.Createlogfile()
	csvReader := csv.NewReader(reader)
	var data []Contacts
	id := 0 // Declare the id variable outside the loop

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log1.Error(err.Error())
			return nil, err
		}

		if len(record) != 3 { // Expecting four columns per row
			log1.Warning("invalid csv format")
			return nil, fmt.Errorf("invalid CSV format")
		}

		id++ // Increment the id for each row
		name := record[0]
		email := record[1]
		json := record[2]

		data = append(data, Contacts{ID: id, Name: name, Email: email, Details: json})
	}

	return data, nil
}
