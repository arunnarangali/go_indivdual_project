package api

import (
	"datastream/dataprocessing"
	"datastream/logs"
	"datastream/service"
	"datastream/types"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"mime/multipart"
	"strings"

	"net/http"
	"os"
	"sync"
)

var uploadPage = template.Must(template.ParseFiles("/home/arun/test4/go_indivdual_project/templates/HomePage.html"))

func HomePageHandler(w http.ResponseWriter, r *http.Request) {

	tmpl, err := template.ParseFiles("/home/arun/test4/go_indivdual_project/templates/HomePage.html")
	if err != nil {
		logs.Logger.Error("error in get homepage", err)
		return
	}
	err = tmpl.Execute(w, nil)
	if err != nil {
		logs.Logger.Error("error in load page", err)
	}
}

func UploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {

		file, header, err := r.FormFile("csvfile")
		if err != nil {
			logs.Logger.Error("Error", err)
			RespondWithError(w, "please choose csv file")
			return
		}
		defer file.Close()

		if header.Header.Get("Content-Type") != "text/csv" {
			logs.Logger.Warning("this not csv file")
			RespondWithError(w, "file is not csv")
			return
		}

		if err := CheckFile(w, file, header); err != nil {
			return
		}
		_, err = file.(io.Seeker).Seek(0, 0)
		if err != nil {
			RespondWithError(w, "Error seeking file: "+err.Error())
			return
		}

		originalFile, err := os.Create("original.csv")
		if err != nil {
			logs.Logger.Error("unable to open the original file", err)
			return
		}
		defer originalFile.Close()

		// Copy the contents of the uploaded file to the original file
		_, err = io.Copy(originalFile, file)
		if err != nil {
			logs.Logger.Error("Error copying file Contents", err)
			return
		}
		http.Redirect(w, r, "/resultpage", http.StatusSeeOther)

		go Readfile()
		return
	}
	logs.Logger.Warning("Use POST method upload a csv file")
	fmt.Fprintln(w, "Use POST method to upload a CSV file")
}

func RespondWithError(w http.ResponseWriter, message string) {

	data := struct {
		Error string
	}{Error: message}

	if err := uploadPage.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func CheckFile(w http.ResponseWriter, file io.Reader, handler *multipart.FileHeader) error {

	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {

			RespondWithError(w, "Error reading CSV: "+err.Error())
			return errors.New("error reading CSVs")
		}
		if len(record) != 3 {
			RespondWithError(w, "Invalid type CSV.Only support name,email and Details")
			return errors.New("invalid type CSV.Only support name,email and Details")
		}

		// Check email format
		if !isValidEmail(record[1]) {
			RespondWithError(w, "Invalid Type Email Format")
			return errors.New("invalid Type Email Format")
		}

		// Check if there is any whitespaces
		if (record[0] == "") || (record[1] == "") || (record[2] == "") {
			RespondWithError(w, "FIle Conatin White Spaces")
			return errors.New("FIle Conatin White Spaces")
		}

	}
	return nil

}

//check email is valid

func isValidEmail(email string) bool {
	return strings.Contains(email, "@") && strings.Contains(email, ".")
}

func Readfile() error {
	filePath := "original.csv"

	file, err := os.Open(filePath)
	if err != nil {
		logs.Logger.Error("unable to open the file:", err)
		return fmt.Errorf("unable to open the file: %v", err)
	}
	defer file.Close()

	//  process the opened CSV file directly
	csvData, err := types.MyCSVReader{}.ReadCSV(file)
	if err != nil {
		logs.Logger.Error("error procesing Csv file in handler :", err)
		return fmt.Errorf("error processing CSV file: %v", err)
	}

	if err := processCSVData(csvData); err != nil {
		logs.Logger.Error("processCsvdata:", err)
		return err
	}

	// Run the Kafka consumer for Contacts
	if err := runKafkaConsumerForContacts(); err != nil {
		logs.Logger.Error("runkafakaConsumeforCOntacts:", err)
		return err
	}

	// Run the Kafka consumer for Activity
	if err := runKafkaConsumerForActivity(); err != nil {
		logs.Logger.Error("runkafakaConsumeForActivity:", err)
		return err
	}

	return nil
}

func processCSVData(csvData []types.Contacts) error {
	var wg sync.WaitGroup

	wg.Add(1)
	go generateActivitiesInBackground(csvData, &wg)

	wg.Wait()
	produceEofmsg()
	return nil
}

func generateActivitiesInBackground(csvData []types.Contacts, wg *sync.WaitGroup) {
	defer logs.Logger.Info("generate activity func stopped\n")
	defer wg.Done()

	var contactStatuses []string
	var activitiesStrings []string

	for _, row := range csvData {
		contactStatus, activitiesString := dataprocessing.CallActivity(row.ID, row)
		contactStatuses = append(contactStatuses, contactStatus)
		activitiesStrings = append(activitiesStrings, activitiesString)
	}

	if err := service.RunKafkaProducerContacts(contactStatuses); err != nil {
		logs.Logger.Error("Error running Kafka producer for contacts:", err)
	}

	if err := service.RunKafkaProducerActivity(activitiesStrings); err != nil {
		logs.Logger.Error("Error running Kafka producer for activities:", err)
	}
}

func produceEofmsg() {
	if err := service.RunKafkaProducerContacts([]string{"Eof"}); err != nil {
		logs.Logger.Error("Error running Kafka producer for eof:", err)
	}

	if err := service.RunKafkaProducerActivity([]string{"Eof"}); err != nil {
		logs.Logger.Error("Error running Kafka producer for eof:", err)
	}
}

func runKafkaConsumerForContacts() error {
	if err := service.RunKafkaConsumerContacts(); err != nil {
		logs.Logger.Error("error running Kafka consumer for Contacts", err)
		return fmt.Errorf("error running Kafka consumer for Contacts: %v", err)
	}
	return nil
}

// Separate function for running Kafka consumer for Activity
func runKafkaConsumerForActivity() error {
	if err := service.RunKafkaConsumerActivity(); err != nil {
		logs.Logger.Error("error running Kafka consumer for Activity: ", err)
		return fmt.Errorf("error running Kafka consumer for Activity: %v", err)
	}
	return nil
}

func ResultpageHandler(w http.ResponseWriter, r *http.Request) {
	// Serve the HTML page with the button
	tmpl, err := template.ParseFiles("/home/arun/test 3/go_indivdual_project/templates/ResultPage.html")
	if err != nil {
		logs.Logger.Error("Internal Server Error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl.Execute(w, nil)
}

func ResultHandler(w http.ResponseWriter, r *http.Request) {
	results, err := service.QueryTopContactActivity()
	if err != nil {
		logs.Logger.Error("Error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert results to JSON
	jsonResponse, err := json.Marshal(results)
	if err != nil {
		logs.Logger.Error("Error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}
