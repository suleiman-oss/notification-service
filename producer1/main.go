package main

import (
	"dependencies/notificationService/producer1/database"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	producer1, _ := CreateProducer()
	defer producer1.Close()
	db, _ := database.DatabaseInit()
	defer db.Close()
	r := mux.NewRouter()
	r.HandleFunc("/general", Producer1Handler(producer1, db)).Methods("POST")
	log.Fatal(http.ListenAndServe(":8001", r))
}
