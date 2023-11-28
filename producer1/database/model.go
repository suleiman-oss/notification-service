package database

import "github.com/jinzhu/gorm"

type GenMessage struct {
	gorm.Model
	Message string `json:"message"`
}

func SaveMessage(db *gorm.DB, msg GenMessage) error {
	err := db.Create(&msg).Error
	if err != nil {
		return err
	}
	return nil
}

func GetLastMessage(db *gorm.DB) (GenMessage, error) {
	var msg GenMessage
	if err := db.Last(&msg).Error; err != nil {
		return GenMessage{}, err
	}
	return msg, nil
}
