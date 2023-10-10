package main

import (
	"datastream/logs"
	route "datastream/routes"
	"net/http"
)

var Logger *logs.SimpleLogger

func main() {
	// Set up your routes
	route.SetupRoutes()

	// Initialize the logger
	Logger = logs.Createlogfile()

	// Serve static files from the "static" directory
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	// Start the HTTP server on port 8080
	http.ListenAndServe(":8080", nil)
}
