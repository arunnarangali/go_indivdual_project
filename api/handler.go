package api

import (
	"datastream/database"
	"datastream/dataprocessing"
	"datastream/logs"
	"datastream/types"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

func HomePageHandler(w http.ResponseWriter, r *http.Request) {

	log1 := logs.Createlogfile()
	tmpl, err := template.ParseFiles("templates/HomePage.html")
	if err != nil {
		// Log the error using the logger
		log1.Error(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = tmpl.Execute(w, nil)
	if err != nil {
		// Log the error using the logger
		log1.Error(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func ResultPageHandler() {
}

func UploadToKafka() {
	// Implement the logic to upload data to Kafka here
}

func GetDataFromClickHouse() {
	// Implement the logic to get data from ClickHouse here
}

func generateActivitiesInBackground(csvData []types.Contacts, wg *sync.WaitGroup) {
	defer fmt.Printf("generate activity func stopped\n")
	defer wg.Done() // Decrease the wait group counter when the goroutine completes
	for _, row := range csvData {
		fmt.Printf("{%d,%s, %s, %s}\n", row.ID, row.Name, row.Email, row.Details)
		//   conta	dataprocessing.CallActivity(row.ID, row)
		contactStatus, activitiesString := dataprocessing.CallActivity(row.ID, row)
		if err := database.RunKafkaProducerContacts(contactStatus); err != nil {
			log.Fatalf("Error running Kafka producer: %v", err)
		}

		if err := database.RunKafkaProducerActivity(activitiesString); err != nil {
			log.Fatalf("Error running Kafka producer: %v", err)
		}
	}
}

func produceEofmsg() {

	if err := database.RunKafkaProducerContacts("Eof"); err != nil {
		log.Fatalf("Error running Kafka producer: %v", err)
	}

	if err := database.RunKafkaProducerActivity("Eof"); err != nil {
		log.Fatalf("Error running Kafka producer: %v", err)
	}
}

func redirectToSuccessPage(w http.ResponseWriter, r *http.Request, data []types.Contacts) {
	log1 := logs.Createlogfile()

	fmt.Println("Redirecting to success page") // Add this line for debugging
	// Parse the HTML template from a file (assuming you have a file named "success.html" with the template)
	tmpl, err := template.ParseFiles("templates/success.html")
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log1.Error(err.Error())
		return
	}

	// Execute the template with the provided data and write it to the response writer
	err = tmpl.Execute(w, data)
	if err != nil {
		log1.Error(err.Error())
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func HandleUpload(w http.ResponseWriter, r *http.Request) {

	if r.Method == http.MethodPost {

		file, _, err := r.FormFile("csvfile")
		if err != nil {
			http.Error(w, "Unable to get the file", http.StatusBadRequest)

			return
		}
		defer file.Close()

		// You can now process the uploaded CSV file directly
		csvData, err := types.MyCSVReader{}.ReadCSV(file)
		if err != nil {
			http.Error(w, "Error processing CSV file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Printf("Before generateActivitiesInBackground")

		var wg sync.WaitGroup
		wg.Add(1) // Add one to the wait group for the generateActivitiesInBackground goroutine
		go generateActivitiesInBackground(csvData, &wg)

		// Wait for all activities to be generated
		wg.Wait()
		produceEofmsg() // call for produec eof msg
		redirectToSuccessPage(w, r, csvData)
		fmt.Print("After redirectToSuccessPage")

		// Run the Kafka consumer function.
		if err := database.RunKafkaConsumerContacts(); err != nil {
			log.Fatalf("Error running Kafka consumer: %v", err)
		}

		// Run the Kafka consumer function.
		if err := database.RunKafkaConsumerActivity(); err != nil {
			log.Fatalf("Error running Kafka consumer: %v", err)
		}

		return
	}

	// If the request method is not POST, you can provide an informative message
	fmt.Fprintln(w, "Use POST method to upload a CSV file")
}

func UploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		// Parse the form data to retrieve the uploaded file
		file, _, err := r.FormFile("csvfile")
		if err != nil {
			http.Error(w, "Unable to get the file", http.StatusBadRequest)
			return
		}
		defer file.Close()

		// Open the original file for writing
		originalFile, err := os.Create("original.csv")
		if err != nil {
			http.Error(w, "Unable to open the original file", http.StatusInternalServerError)
			return
		}
		defer originalFile.Close()

		// Copy the contents of the uploaded file to the original file
		_, err = io.Copy(originalFile, file)
		if err != nil {
			http.Error(w, "Error copying file contents", http.StatusInternalServerError)
			return
		}

		fmt.Fprintln(w, "CSV file has been updated successfully")
		go Readfile()

		return
	}

	// If the request method is not POST, you can provide an informative message
	fmt.Fprintln(w, "Use POST method to upload a CSV file")
}

// Assuming csvData is a []types.Contacts
func processCSVData(csvData []types.Contacts) error {
	var wg sync.WaitGroup

	wg.Add(1) // Add one to the wait group for the generateActivitiesInBackground goroutine
	generateActivitiesInBackground(csvData, &wg)

	// Wait for all activities to be generated
	wg.Wait()
	produceEofmsg() // call for produce eof msg
	return nil
}

// Separate function for running Kafka consumer for Contacts
func runKafkaConsumerForContacts() error {
	if err := database.RunKafkaConsumerContacts(); err != nil {
		return fmt.Errorf("error running Kafka consumer for Contacts: %v", err)
	}
	return nil
}

// Separate function for running Kafka consumer for Activity
func runKafkaConsumerForActivity() error {
	if err := database.RunKafkaConsumerActivity(); err != nil {
		return fmt.Errorf("error running Kafka consumer for Activity: %v", err)
	}
	return nil
}

// Modified Readfile function
func Readfile() error {
	filePath := "/home/arun/Documents/project/project/go_indivdual_project/original.csv"

	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("unable to open the file: %v", err)
	}
	defer file.Close()

	// You can now process the opened CSV file directly
	csvData, err := types.MyCSVReader{}.ReadCSV(file)
	if err != nil {
		return fmt.Errorf("error processing CSV file: %v", err)
	}

	// Process CSV data concurrently
	if err := processCSVData(csvData); err != nil {
		return err
	}

	// Run the Kafka consumer for Contacts
	if err := runKafkaConsumerForContacts(); err != nil {
		return err
	}

	// Run the Kafka consumer for Activity
	if err := runKafkaConsumerForActivity(); err != nil {
		return err
	}

	// Continue with the rest of your code

	return nil // Return nil to indicate success
}
