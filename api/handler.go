package api

import (
	"datastream/database"
	"datastream/dataprocessing"
	"datastream/logs"
	"datastream/types"
	"fmt"
	"html/template"
	"net/http"
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
		fmt.Println(contactStatus)
		fmt.Println("Activities:")
		fmt.Println(activitiesString)
		err := database.ProduceKafkaMessageActivity(activitiesString)
		if err != nil {
			// logs.Fatalf("Error producing Kafka message: %v", err)
			fmt.Printf("Error producing kafka message %v", err)
		}
		err = database.ProduceKafkaMessageContacts(contactStatus)
		if err != nil {
			// logs.Fatalf("Error producing Kafka message: %v", err)
			fmt.Printf("Error producing kafka message %v", err)
		}

	}
}

func Eof() {
	err := database.ProduceKafkaMessageActivity("Eof")
	if err != nil {
		// logs.Fatalf("Error producing Kafka message: %v", err)
		fmt.Printf("Error producing kafka message %v", err)
	}
	err = database.ProduceKafkaMessageContacts("Eof")
	if err != nil {
		// logs.Fatalf("Error producing Kafka message: %v", err)
		fmt.Printf("Error producing kafka message %v", err)
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
	log1 := logs.Createlogfile()
	if r.Method == http.MethodPost {
		// Parse the form data to retrieve the uploaded file
		err := r.ParseMultipartForm(10 << 20) // 10 MB maximum file size
		if err != nil {
			http.Error(w, "Unable to parse form", http.StatusBadRequest)
			log1.Error(err.Error())
			return
		}

		file, _, err := r.FormFile("csvfile")
		if err != nil {
			http.Error(w, "Unable to get the file", http.StatusBadRequest)
			log1.Error(err.Error())
			return
		}
		defer file.Close()

		// You can now process the uploaded CSV file directly
		csvData, err := types.MyCSVReader{}.ReadCSV(file)
		if err != nil {
			log1.Error(err.Error())
			http.Error(w, "Error processing CSV file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Printf("Before generateActivitiesInBackground")

		var wg sync.WaitGroup
		wg.Add(1) // Add one to the wait group for the generateActivitiesInBackground goroutine
		go generateActivitiesInBackground(csvData, &wg)

		// Wait for all activities to be generated
		wg.Wait()
		Eof()
		fmt.Printf("After generateActivitiesInBackground")

		fmt.Printf("Before redirectToSuccessPage")
		redirectToSuccessPage(w, r, csvData)
		fmt.Print("After redirectToSuccessPage")
		mysqlConnector, err := database.ConfigureMySQLDB()
		if err != nil {
			fmt.Printf("Error configuring MySQL database: %v\n", err)
			return
		}

		err = database.ConsumeKafkaMessages(mysqlConnector)
		if err != nil {
			fmt.Printf("Error consuming Kafka messages: %v\n", err)
		}

		mysqlConnector, err = database.ConfigureMySQLDB()
		if err != nil {
			fmt.Printf("Error configuring MySQL database: %v\n", err)
			return
		}

		err = database.ConsumeKafkaMessagesContact(mysqlConnector)
		if err != nil {
			fmt.Printf("Error consuming Kafka messages: %v\n", err)
		}
		fmt.Printf("inserted")
		return
	}

	// If the request method is not POST, you can provide an informative message
	fmt.Fprintln(w, "Use POST method to upload a CSV file")
}
