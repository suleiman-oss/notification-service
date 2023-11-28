package database

import (
	"fmt"

	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
)

func DatabaseInit() (*gorm.DB, error) {
	var (
		host     = "127.0.0.1"
		port     = "5432"
		user     = "postgres"
		dbname   = "mydb"
		password = "1234"
	)

	conn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable",
		host,
		port,
		user,
		dbname,
		password,
	)

	db, err := gorm.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	return db, nil
}
func Migrate(db *gorm.DB) error {
	err := db.AutoMigrate(&Consumer2Message{}).Error
	if err != nil {
		return err
	}
	return nil
}
