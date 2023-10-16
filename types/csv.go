package types

import (
	crypto "crypto/rand"
	"datastream/logs"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
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

	csvReader := csv.NewReader(reader)
	var data []Contacts

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			logs.Logger.Error("Error in reading", err)
			return nil, err
		}

		if len(record) != 3 { // Expecting four columns per row
			logs.Logger.Warning("invalid csv format")
			return nil, fmt.Errorf("invalid CSV format")
		}

		id := generateRandomID()
		name := record[0]
		email := record[1]
		json := record[2]
		contact := Contacts{ID: id, Name: name, Email: email, Details: json}
		data = append(data, contact)
	}

	return data, nil
}

func generateRandomID() string {

	uuidObj, err := uuid.NewRandom()

	if err != nil {

		logs.Logger.Error("Error creating id", err)

	}

	randomBytes := make([]byte, 8)

	_, err = io.ReadFull(crypto.Reader, randomBytes) // Use rand.Reader from crypto/rand

	if err != nil {

		logs.Logger.Error("Error creating id", err)

	}

	randomString := fmt.Sprintf("%s-%x", uuidObj, randomBytes)

	randomString = strings.ReplaceAll(randomString, "-", "")

	return randomString

}
