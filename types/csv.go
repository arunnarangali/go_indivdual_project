package types

import (
	crypto "crypto/rand"
	"datastream/logs"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

type ErrorData struct {
	Error string
}

type CSVReader interface {
	ReadCSV(reader io.Reader) ([]Contacts, error)
}

type MyCSVReader struct{}

func (r MyCSVReader) ReadCSV(reader io.Reader, batchSize int, resultCh chan []Contacts) {
	errlog := logs.Createlogfile()
	csvReader := csv.NewReader(reader)
	var data []Contacts

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			if len(data) > 0 {
				resultCh <- data // Send any remaining data as the last batch
			}
			close(resultCh) // Close the result channel to signal the end of processing
			return
		}
		if err != nil {
			logs.Logger.Error("Error in reading", err)
			return
		}

		if len(record) != 3 {
			logs.Logger.Warning("invalid CSV format")
			return
		}

		id := generateRandomID()
		name := record[0]
		email := record[1]
		json := record[2]
		if (record[0] == "") || (record[1] == "") || (record[2] == "") {
			errlog.Warning("FIle Conatin White Spaces")
			continue
		}

		if isValidName(name) && isValidEmail(email) && isValidJSON(json) {
			contact := Contacts{ID: id, Name: name, Email: email, Details: json}
			data = append(data, contact)

			if len(data) == batchSize {
				resultCh <- data
				data = nil
			}
		} else {
			errlog.Warning(fmt.Sprintf("invalid csv record name:%s email:%s Details:%s", name, email, json))
			logs.Logger.Warning(fmt.Sprintf("invalid csv record name:%s email:%s Details:%s", name, email, json))
		}
	}

}

func isValidName(name string) bool {
	for _, char := range name {
		if (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') && char != ' ' {
			return false
		}
	}
	return true
}
func isValidJSON(jsonStr string) bool {
	var result map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &result)
	return err == nil
}

func isValidEmail(email string) bool {
	emailPattern := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
	match, _ := regexp.MatchString(emailPattern, email)
	return match
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
