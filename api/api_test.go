package api_test

import (
	"bytes"
	"datastream/api"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"
)

func TestUploadHandler(t *testing.T) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	partHeader := make(textproto.MIMEHeader)
	partHeader.Set("Content-Type", "text/csv")

	part, err := writer.CreatePart(partHeader)
	if err != nil {
		t.Fatalf("Error creating form file: %v", err)
	}
	_, _ = part.Write([]byte("file content"))

	writer.Close()

	req := httptest.NewRequest("POST", "/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	recorder := httptest.NewRecorder()

	api.UploadHandler(recorder, req)

	if recorder.Code != http.StatusSeeOther {
		t.Errorf("Expected status code %d, but got %d", http.StatusSeeOther, recorder.Code)
	}

}

// func TestUploadHandler(t *testing.T) {
// 	// Create a valid CSV content
// 	csvContent := `John,john@example.com,{"dob": "2019-03-11", "city": "City50", "country": "Country50"}`
// 	body := &bytes.Buffer{}
// 	writer := multipart.NewWriter(body)

// 	// Create a form file part with a different name to match the form in your handler
// 	part, err := writer.CreateFormFile("csvfile", "sample.csv")
// 	if err != nil {
// 		t.Fatalf("Error creating form file: %v", err)
// 	}

// 	// Set the Content-Type of the file to "text/csv"
// 	part.Header.Set("Content-Type", "text/csv")

// 	_, _ = part.Write([]byte(csvContent))

// 	writer.Close()

// 	// Create a new request with the correct URL
// 	req := httptest.NewRequest("POST", "/upload", body)
// 	req.Header.Set("Content-Type", writer.FormDataContentType())

// 	recorder := httptest.NewRecorder()

// 	// Call the handler
// 	UploadHandler(recorder, req)

// 	// Check if the status code is what you expect
// 	if recorder.Code != http.StatusSeeOther {
// 		t.Errorf("Expected status code %d, but got %d", http.StatusSeeOther, recorder.Code)
// 	}

// 	// You can also check for the existence of the "original.csv" file
// 	_, err = os.Stat("/home/arun/test 5/go_indivdual_project/original.csv")
// 	if os.IsNotExist(err) {
// 		t.Errorf("Expected 'original.csv' file to exist, but it does not.")
// 	}

// }

func TestHomePageHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.HomePageHandler)

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
	handler := http.HandlerFunc(api.ResultpageHandler)

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v, want %v", status, http.StatusOK)
	}

}

// type MockService struct{}

// func (s *MockService) QueryTopContactActivity(query string) ([]ContactActivity, error) {
// 	// You can mock the behavior of your service here.
// 	// For testing, you can return predefined data or errors as needed.
// 	return []ContactActivity{{ContactsID: 1, Clicked: 5}}, nil
// }

// func TestResultHandler_Query1(t *testing.T) {
// 	// Create a request with button value "query1".
// 	req := httptest.NewRequest("GET", "/result?button=query1", nil)
// 	rr := httptest.NewRecorder()

// 	handler := http.HandlerFunc(api.ResultHandler)

// 	handler.ServeHTTP(rr, req)

// 	if rr.Code != http.StatusOK {
// 		t.Errorf("Expected status code %d, but got %d", http.StatusOK, rr.Code)
// 	}

// 	expectedJSON := `[{"ContactsID":1,"Clicked":5}]`
// 	if strings.TrimSpace(rr.Body.String()) != expectedJSON {
// 		t.Errorf("Expected response body to be %s, but got %s", expectedJSON, rr.Body.String())
// 	}
// }

// func TestResultHandler_Query2(t *testing.T) {
// 	// Create a request with button value "query2".
// 	req := httptest.NewRequest("GET", "/your-endpoint?button=query2", nil)
// 	rr := httptest.NewRecorder()

// 	// Create a service and pass it to the handler.
// 	service := &MockService{}
// 	handler := http.HandlerFunc(api.ResultHandler)

// 	handler.ServeHTTP(rr, req)

// 	if rr.Code != http.StatusOK {
// 		t.Errorf("Expected status code %d, but got %d", http.StatusOK, rr.Code)
// 	}

// 	expectedJSON := `[{"ContactsID":1,"Clicked":5}]`
// 	if strings.TrimSpace(rr.Body.String()) != expectedJSON {
// 		t.Errorf("Expected response body to be %s, but got %s", expectedJSON, rr.Body.String())
// 	}
// }

// func TestResultHandler_InvalidButtonValue(t *testing.T) {
// 	// Create a request with an invalid button value.
// 	req := httptest.NewRequest("GET", "/your-endpoint?button=invalid", nil)
// 	rr := httptest.NewRecorder()

// 	// Create a service and pass it to the handler.
// 	service := &MockService{}
// 	handler := http.HandlerFunc(ResultHandler)

// 	handler.ServeHTTP(rr, req)

// 	if rr.Code != http.StatusBadRequest {
// 		t.Errorf("Expected status code %d, but got %d", http.StatusBadRequest, rr.Code)
// 	}

// 	expectedJSON := "Invalid button value"
// 	if strings.TrimSpace(rr.Body.String()) != expectedJSON {
// 		t.Errorf("Expected response body to be %s, but got %s", expectedJSON, rr.Body.String())
// 	}
// }

// func TestResultHandler_ServiceError(t *testing.T) {
// 	// Create a request with button value "query1".
// 	req := httptest.NewRequest("GET", "/your-endpoint?button=query1", nil)
// 	rr := httptest.NewRecorder()

// 	// Create a service that returns an error and pass it to the handler.
// 	service := &MockService{}
// 	handler := http.HandlerFunc(ResultHandler)

// 	// Force an error by passing an empty query to the service.
// 	service.(*MockService).QueryTopContactActivity = func(query string) ([]ContactActivity, error) {
// 		return nil, errors.New("Service error")
// 	}

// 	handler.ServeHTTP(rr, req)

// 	if rr.Code != http.StatusInternalServerError {
// 		t.Errorf("Expected status code %d, but got %d", http.StatusInternalServerError, rr.Code)
// 	}

// 	expectedJSON := "Service error"
// 	if strings.TrimSpace(rr.Body.String()) != expectedJSON {
// 		t.Errorf("Expected response body to be %s, but got %s", expectedJSON, rr.Body.String())
// 	}
// }
