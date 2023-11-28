package main

import (
	"dependencies/notificationService/consumer2/database"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

func main() {
	con, err := CreateConsumer()
	if err != nil {
		log.Fatal("Error while creating Kafka consumer 2")
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
	r.HandleFunc("/consume/urgent", Consumer1Handler(con, &wg, db)).Methods("GET")
	go ConsumerMessages(con, &wg, db)
	log.Fatal(http.ListenAndServe("localhost:8004", r))
}
