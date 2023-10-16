package main

import (
	"datastream/routes"
	"net/http"
)

func main() {
	// Set up your routes
	routes.SetupRoutes()

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	// Start the HTTP server on port 8080
	http.ListenAndServe(":8080", nil)
}
