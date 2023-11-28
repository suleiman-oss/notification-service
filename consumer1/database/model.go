package database

import "github.com/jinzhu/gorm"

type Consumer1Message struct {
	gorm.Model
	Message string `json:"message"`
}

func SaveMessage(db *gorm.DB, msg Consumer1Message) error {
	err := db.Create(&msg).Error
	if err != nil {
		return err
	}
	return nil
}

func GetLastMessage(db *gorm.DB) (Consumer1Message, error) {
	var msg Consumer1Message
	err := db.Last(&msg).Error
	if err != nil {
		return Consumer1Message{}, err
	}
	return msg, nil
}
