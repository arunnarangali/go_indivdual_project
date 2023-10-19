package main

import (
	"datastream/routes"
	"datastream/service"
	"net/http"
)

func main() {
	// Set up your routes
	routes.SetupRoutes()

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	go service.RunKafkaConsumerContacts()

	go service.RunKafkaConsumerActivity()
	http.ListenAndServe(":8082", nil)

}
