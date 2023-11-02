package api

import (
	"datastream/Process"
	"datastream/dataprocessing"
	"datastream/logs"
	"datastream/service"

	"datastream/types"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"strings"
	"sync"
	"time"

	"net/http"
	"os"
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
			logs.Logger.Error("Error in get", err)
			RespondWithError(w, "please choose csv file")
			return
		}

		defer file.Close()

		if !strings.HasSuffix(header.Filename, ".csv") {
			logs.Logger.Warning("this is not a CSV file")
			RespondWithError(w, "file is not a CSV")
			return
		}
		filePath := "original.csv"
		originalFile, err := os.Create("original.csv")
		if err != nil {
			logs.Logger.Error("unable to open the original file", err)
			return
		}
		defer originalFile.Close()

		_, err = io.Copy(originalFile, file)
		if err != nil {
			logs.Logger.Error("Error copying file Contents", err)
			return
		}
		go Readfile(filePath)
		time.Sleep(15 * time.Second)
		http.Redirect(w, r, "/resultpage", http.StatusSeeOther)
		return
	}

	logs.Logger.Warning("Use POST method upload a csv file")
}

func RespondWithError(w http.ResponseWriter, message string) {

	data := types.ErrorData{Error: message}

	if err := uploadPage.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func Readfile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		logs.Logger.Error("unable to open the file:", err)
		return err
	}
	defer file.Close()

	batchSize := 25

	resultCh := make(chan []types.Contacts)

	r := types.MyCSVReader{}
	go r.ReadCSV(file, batchSize, resultCh)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for batch := range resultCh {
			if err := ProcessCSVData(batch); err != nil {
				logs.Logger.Error("processCsvdata:", err)
			}
		}
	}()
	wg.Wait()
	return nil
}

func ProcessCSVData(csvData []types.Contacts) error {
	var wg sync.WaitGroup

	wg.Add(1)
	go generateActivitiesInBackground(csvData, &wg)

	wg.Wait()
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

	go Process.RunKafkaConsumerContacts("kafka")

	go Process.RunKafkaConsumerActivity("kafka")

	if err := Process.RunKafkaProducerContacts(contactStatuses); err != nil {
		logs.Logger.Error("Error running Kafka producer for contacts:", err)
	}
	if err := Process.RunKafkaProducerActivity(activitiesStrings); err != nil {
		logs.Logger.Error("Error running Kafka producer for activities:", err)
	}

}

func ResultpageHandler(w http.ResponseWriter, r *http.Request) {

	// Serve the HTML page with the button
	tmpl, err := template.ParseFiles("/home/arun/test 5/go_indivdual_project/templates/ResultPage.html")
	if err != nil {
		logs.Logger.Error("Internal Server Error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl.Execute(w, nil)
}

func ResultHandler(w http.ResponseWriter, r *http.Request) {

	buttonValue := r.FormValue("button")
	var query string

	if buttonValue == "query1" {
		query = `
		SELECT ContactsID, clicked
		FROM arun_campaign.contact_activity_summary_mv_last_three_month_summery  FINAL
		ORDER BY clicked DESC
		LIMIT 5
	`

	}
	if buttonValue == "query2" {
		query = `
		SELECT ContactsID, clicked
		FROM arun_campaign.contact_activity_summary_mv_last_three_month_summery  FINAL
		ORDER BY clicked DESC
		LIMIT 10
	`
	}

	if query == "" {
		http.Error(w, "Invalid button value", http.StatusBadRequest)
		return
	}

	results, err := service.QueryTopContactActivity(query)
	if err != nil {
		logs.Logger.Error("Error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonData, err := json.Marshal(results)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)

}
