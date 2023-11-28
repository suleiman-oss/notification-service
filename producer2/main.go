package main

import (
	"dependencies/notificationService/producer2/database"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	producer2, _ := CreateProducer()
	defer producer2.Close()
	db, _ := database.DatabaseInit()
	defer db.Close()
	r := mux.NewRouter()
	r.HandleFunc("/produce/urgent", Producer1Handler(producer2, db)).Methods("POST")
	log.Fatal(http.ListenAndServe(":8002", r))
}
