package database

import "github.com/jinzhu/gorm"

type UrgMessage struct {
	gorm.Model
	Message string `json:"message"`
}

func SaveMessage(db *gorm.DB, msg UrgMessage) error {
	err := db.Create(&msg).Error
	if err != nil {
		return err
	}
	return nil
}

func GetLastMessage(db *gorm.DB) (UrgMessage, error) {
	var msg UrgMessage
	if err := db.Last(&msg).Error; err != nil {
		return UrgMessage{}, err
	}
	return msg, nil
}
