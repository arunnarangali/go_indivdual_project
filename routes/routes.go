package route

import (
	"datastream/api"
	"net/http"
)

func SetupRoutes() {
	http.HandleFunc("/", api.HomePageHandler)
	http.HandleFunc("/upload", api.HandleUpload)
}
