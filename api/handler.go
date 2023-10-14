package api

import (
	"datastream/dataprocessing"
	"datastream/logs"
	"datastream/service"
	"datastream/types"
	"encoding/json"
	"fmt"
	"html/template"
	"io"

	"net/http"
	"os"
	"sync"
)

func HomePageHandler(w http.ResponseWriter, r *http.Request) {

	tmpl, err := template.ParseFiles("templates/HomePage.html")
	if err != nil {
		logs.Logger.Error("error in get homepage", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = tmpl.Execute(w, nil)
	if err != nil {
		logs.Logger.Error("error in load page", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func UploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		file, _, err := r.FormFile("csvfile")
		if err != nil {
			logs.Logger.Error("Unable to get the file", err)
			return
		}
		defer file.Close()

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

		go Readfile()
		http.Redirect(w, r, "/resultpage", http.StatusSeeOther)
		return
	}
	logs.Logger.Warning("Use POST method upload a csv file")
	fmt.Fprintln(w, "Use POST method to upload a CSV file")
}

func generateActivitiesInBackground(csvData []types.Contacts, wg *sync.WaitGroup) {
	defer logs.Logger.Info("generate activity func stopped\n")
	defer wg.Done()

	var contactStatuses []string
	var activitiesStrings []string

	for _, row := range csvData {
		// fmt.Printf("{%s,%s, %s, %s}\n", row.ID, row.Name, row.Email, row.Details)

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

func processCSVData(csvData []types.Contacts) error {
	var wg sync.WaitGroup

	wg.Add(1)
	go generateActivitiesInBackground(csvData, &wg)

	wg.Wait()
	produceEofmsg()
	return nil
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

// Modified Readfile function
func Readfile() error {
	filePath := "original.csv"

	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		logs.Logger.Error("unable to open the file:", err)
		return fmt.Errorf("unable to open the file: %v", err)
	}
	defer file.Close()

	// You can now process the opened CSV file directly
	csvData, err := types.MyCSVReader{}.ReadCSV(file)
	if err != nil {
		logs.Logger.Error("error procesing Csv file in handler :", err)
		return fmt.Errorf("error processing CSV file: %v", err)
	}

	// Process CSV data concurrently
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

	return nil // Return nil to indicate success
}

func ResultpageHandler(w http.ResponseWriter, r *http.Request) {
	// Serve the HTML page with the button
	tmpl, err := template.ParseFiles("templates/ResultPage.html")
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
