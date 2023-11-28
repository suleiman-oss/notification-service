package database

import "github.com/jinzhu/gorm"

type Consumer2Message struct {
	gorm.Model
	Message string `json:"message"`
}

func SaveMessage(db *gorm.DB, msg Consumer2Message) error {
	err := db.Create(&msg).Error
	if err != nil {
		return err
	}
	return nil
}

func GetLastMessage(db *gorm.DB) (Consumer2Message, error) {
	var msg Consumer2Message
	err := db.Last(&msg).Error
	if err != nil {
		return Consumer2Message{}, err
	}
	return msg, nil
}
