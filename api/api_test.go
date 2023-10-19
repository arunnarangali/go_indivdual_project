package api

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestUploadHandler(t *testing.T) {
	// Create a valid CSV content
	csvContent := `John,john@example.com,{"dob": "2019-03-11", "city": "City50", "country": "Country50"}`
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Create a form file part with a different name to match the form in your handler
	part, err := writer.CreateFormFile("csvfile", "sample.csv")
	if err != nil {
		t.Fatalf("Error creating form file: %v", err)
	}

	// Set the Content-Type of the file to "text/csv"
	part.Header.Set("Content-Type", "text/csv")

	_, _ = part.Write([]byte(csvContent))

	writer.Close()

	// Create a new request with the correct URL
	req := httptest.NewRequest("POST", "/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	recorder := httptest.NewRecorder()

	// Call the handler
	UploadHandler(recorder, req)

	// Check if the status code is what you expect
	if recorder.Code != http.StatusSeeOther {
		t.Errorf("Expected status code %d, but got %d", http.StatusSeeOther, recorder.Code)
	}

	// You can also check for the existence of the "original.csv" file
	_, err = os.Stat("/home/arun/test 5/go_indivdual_project/original.csv")
	if os.IsNotExist(err) {
		t.Errorf("Expected 'original.csv' file to exist, but it does not.")
	}

}

func TestHomePageHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(HomePageHandler)

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v, want %v", status, http.StatusOK)
	}
}

func TestResultpageHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/resultpage", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(ResultpageHandler)

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v, want %v", status, http.StatusOK)
	}

}

// func TestResultHandler(t *testing.T) {
// 	req, err := http.NewRequest("GET", "/result", nil)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	rr := httptest.NewRecorder()
// 	handler := http.HandlerFunc(ResultHandler)

// 	// Call the handler
// 	handler.ServeHTTP(rr, req)

// 	if status := rr.Code; status != http.StatusOK {
// 		t.Errorf("Handler returned wrong status code: got %v, want %v", status, http.StatusOK)
// 	}

// 	var response map[string]interface{}
// 	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
// 		t.Errorf("Error unmarshalling JSON response: %v", err)
// 	}
// }
