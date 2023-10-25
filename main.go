package main

import (
	"datastream/routes"
	"net/http"
)

func main() {
	// Set up your routes
	routes.SetupRoutes()

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	http.ListenAndServe(":8082", nil)

}
