package main

import (
	"encoding/json"
	"log"
	"net/http"
	"notification/consumer2/database"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorilla/mux"
	"github.com/jinzhu/gorm"
)

var (
	brokerList = []string{"localhost:9092"}
	topic      = "urgent"
)

func CreateConsumer() (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
func ConsumerMessages(consumer sarama.Consumer, wg *sync.WaitGroup, db *gorm.DB) {
	defer wg.Done()
	part, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Println("Error while partitioning")
		return
	}
	msgChan := part.Messages()
	errChan := part.Errors()
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				return
			}
			handleMsg(msg, db)
		case err := <-errChan:
			log.Printf("Error while reciving messages %s\n", err)
		}
	}
}

func handleMsg(msg *sarama.ConsumerMessage, db *gorm.DB) {
	m := string(msg.Value)
	log.Printf("Received message is %s\n", m)
	if err := database.SaveMessage(db, database.Consumer2Message{Message: m}); err != nil {
		log.Println("Error while saving consumer 2 message to DB")
	}
}

func Consumer1Handler(consumer sarama.Consumer, wg *sync.WaitGroup, db *gorm.DB) http.HandlerFunc {
	wg.Add(1)
	return func(w http.ResponseWriter, r *http.Request) {
		latestMessage, err := database.GetLastMessage(db)
		if err != nil {
			http.Error(w, "Error retrieving latest message from the database", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(latestMessage)
	}
}
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
