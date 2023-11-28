package main

import (
	"dependencies/notificationService/consumer1/database"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/jinzhu/gorm"
)

var (
	brokerList = []string{"localhost:9092"}
	topic      = "general"
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
	if err := database.SaveMessage(db, database.Consumer1Message{Message: m}); err != nil {
		log.Println("Error while saving consumer 1 message to DB")
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
