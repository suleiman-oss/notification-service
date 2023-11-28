package main

import (
	"dependencies/notificationService/producer2/database"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/jinzhu/gorm"
)

var (
	brokerList = []string{"localhost:9092"}
	topic      = "urgent"
)

func CreateProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

func Producer1Handler(producer sarama.SyncProducer, db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var msg database.UrgMessage
		json.NewDecoder(r.Body).Decode(&msg)
		m := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg.Message),
		}
		_, _, err := producer.SendMessage(m)
		if err != nil {
			http.Error(w, "Error sending message to kakfa", 500)
			log.Fatal("Error while sending message to kafka")
			return
		}
		err = database.SaveMessage(db, msg)
		if err != nil {
			log.Printf("Error while saving message to DB %s\n", err.Error())
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "Message sent successfully and saved to the database")
	}
}
