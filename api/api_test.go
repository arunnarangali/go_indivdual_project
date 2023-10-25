package api_test

import (
	"bytes"
	"datastream/api"
	"datastream/types"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sync"
	"testing"
)

func TestUploadHandler(t *testing.T) {
	csvContent := "John,john@example.com,{\"country\":\"usa\",\"city\":\"washington\",\"dob\":\"29-09-2002\"}\n" +
		"Alice,alice@example.com,{\"country\":\"usa\",\"city\":\"washington\",\"dob\":\"29-09-2002\"}\n"

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("csvfile", "test.csv") // Change field name to "csvfile"
	part.Write([]byte(csvContent))
	writer.Close()

	// Create a sample request with the CSV file
	req := httptest.NewRequest("POST", "/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	rr := httptest.NewRecorder() // Initialize the response recorder
	api.UploadHandler(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Errorf("Expected status code %d, but got %d", http.StatusSeeOther, rr.Code)
	}

	// Check the response body
	expectedRedirectURL := "/resultpage"
	if location := rr.Header().Get("Location"); location != expectedRedirectURL {
		t.Errorf("Expected redirect URL %s, but got %s", expectedRedirectURL, location)
	}
}

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

func TestReadfile(t *testing.T) {
	t.Run("Valid CSV", func(t *testing.T) {
		// Create a temporary test CSV file
		csvData := []byte(`
"Dona","dona15@example.com","{""dob"": ""1954-05-08"", ""city"": ""City15"", ""country"": ""Country15""}"`)
		tempFile, err := os.CreateTemp("", "test*.csv")
		if err != nil {
			t.Fatalf("Failed to create temporary file: %v", err)
		}
		defer os.Remove(tempFile.Name())
		tempFile.Write(csvData)
		tempFile.Close()

		// Call the Readfile function with the temporary file path
		err = api.Readfile(tempFile.Name())
		if err != nil {
			t.Fatalf("Readfile returned an error for a valid CSV: %v", err)
		}
	})

	t.Run("Non-existent file", func(t *testing.T) {
		// Test with a non-existent file, should return an error
		err := api.Readfile("nonexistentfile.csv")
		if err == nil {
			t.Errorf("Expected an error for a non-existent file, but got nil")
		}
	})

	t.Run("Edge Case: Empty CSV", func(t *testing.T) {
		// Create an empty CSV file
		tempFile, err := os.CreateTemp("", "test*.csv")
		if err != nil {
			t.Fatalf("Failed to create temporary file: %v", err)
		}
		defer os.Remove(tempFile.Name())
		tempFile.Close()

		// Call the Readfile function with the empty file
		err = api.Readfile(tempFile.Name())
		if err != nil {
			t.Fatalf("Readfile returned an error for an empty CSV: %v", err)
		}
	})
}

func TestProcessCSVData(t *testing.T) {
	// Create a slice of mock CSV data for testing
	csvData := []types.Contacts{
		{
			Name:    "arun",
			Email:   "arunnanrangali123@gmail.com",
			Details: `{"dob": "1954-05-08", "city": "City15", "country": "Country15"}`,
		},
	}

	var wg sync.WaitGroup

	err := api.ProcessCSVData(csvData)

	if err != nil {
		t.Errorf("Expected error to be nil, got %v", err)
	}

	wg.Wait() // Wait for the WaitGroup to finish
}

func TestResultHandler(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		pattern string
		status  int
	}{
		{
			name:    "Query1",
			query:   "?button=query1",
			pattern: `\[{"ContactID":"[a-f0-9]+","Click":\d+},\{"ContactID":"[a-f0-9]+","Click":\d+},\{"ContactID":"[a-f0-9]+","Click":\d+},\{"ContactID":"[a-f0-9]+","Click":\d+},\{"ContactID":"[a-f0-9]+","Click":\d+}\]`,
			status:  http.StatusOK,
		},
		{
			name:    "Query2",
			query:   "?button=query2",
			pattern: `\[{"ContactID":"[a-f0-9]+","Click":\d+},\{"ContactID":"id\d+","Click":\d+},\{"ContactID":"id\d+","Click":\d+},\{"ContactID":"id\d+","Click":\d+},\{"ContactID":"id\d+","Click":\d+},\{"ContactID":"id\d+","Click":\d+},\{"ContactID":"id\d+","Click":\d+},\{"ContactID":"id\d+","Click":\d+},\{"ContactID":"id\d+","Click":\d+},\{"ContactID":"id\d+","Click":\d+}\]`,
			status:  http.StatusOK,
		},
		{
			name:   "InvalidButtonValue",
			query:  "?button=invalid",
			status: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/"+tt.query, nil)
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(api.ResultHandler)

			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tt.status {
				t.Errorf("Handler returned wrong status code for %s: got %v, want %v", tt.name, status, tt.status)
			}

			if tt.pattern != "" {
				match, _ := regexp.MatchString(tt.pattern, rr.Body.String())
				if !match {
					t.Errorf("Handler returned unexpected body pattern for %s: got %v, want %v", tt.name, rr.Body.String(), tt.pattern)
				}
			}
		})
	}
}

// func TestResultHandler(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		query   string
// 		pattern string
// 		status  int
// 	}{
// 		{
// 			name:    "Query1",
// 			query:   "?button=query1",
// 			pattern: `{"ContactID":"[a-zA-Z0-9]+","Click":\d+}`,
// 			status:  http.StatusOK,
// 		},
// 		{
// 			name:    "Query2",
// 			query:   "?button=query2",
// 			pattern: `{"ContactID":"[a-zA-Z0-9]+","Click":\d+}`,
// 			status:  http.StatusOK,
// 		},
// 		{
// 			name:   "InvalidButtonValue",
// 			query:  "?button=invalid",
// 			status: http.StatusBadRequest,
// 		},
// 		// Add more test cases as needed
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			req, err := http.NewRequest("GET", "/"+tt.query, nil)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			rr := httptest.NewRecorder()
// 			handler := http.HandlerFunc(api.ResultHandler)

// 			handler.ServeHTTP(rr, req)

// 			if status := rr.Code; status != tt.status {
// 				t.Errorf("Handler returned wrong status code for %s: got %v, want %v", tt.name, status, tt.status)
// 			}

// 			if tt.pattern != "" {
// 				// Check if the response body matches the expected pattern
// 				var response []map[string]interface{}
// 				if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
// 					t.Errorf("Failed to unmarshal response body: %v", err)
// 				}

// 				for _, item := range response {
// 					for key, value := range item {
// 						stringValue, ok := value.(string)
// 						fmt.Println("hi")
// 						fmt.Println(stringValue)
// 						fmt.Println("hi")

// 						if !ok {
// 							t.Errorf("Value for key %s in %v is not a string", key, item)
// 							continue
// 						}
// 						if match, _ := regexp.MatchString(tt.pattern, stringValue); !match {
// 							t.Errorf("Handler response doesn't match the expected pattern for %s: key %s, value %s, pattern %s", tt.name, key, stringValue, tt.pattern)
// 						}
// 					}
// 				}
// 			}
// 		})
// 	}
// }
