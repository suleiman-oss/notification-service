package main

import (
	"dependencies/notificationService/consumer1/database"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

func main() {
	con, err := CreateConsumer()
	if err != nil {
		log.Fatal("Error while creating Kafka producer")
	}
	defer con.Close()
	db, err := database.DatabaseInit()
	if err != nil {
		log.Fatal("Error connecting to the database")
	}
	defer db.Close()
	err = database.Migrate(db)
	if err != nil {
		log.Fatal("Error running database migrations")
	}
	var wg sync.WaitGroup
	r := mux.NewRouter()
	r.HandleFunc("/consume/general", Consumer1Handler(con, &wg, db)).Methods("GET")
	go ConsumerMessages(con, &wg, db)
	log.Fatal(http.ListenAndServe("localhost:8003", r))
}
