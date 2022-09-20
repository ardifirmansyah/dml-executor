package src

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/jmoiron/sqlx"
)

type DB struct {
	DBConnection *sqlx.DB `json:"-"`
	Conn         string   `json:"-"`
	Host         string   `json:"host"`
	DBName       string   `json:"db_name"`
	Type         string   `json:"type"`
}

var Database *DB

func InitDBConnection(config *Configuration) {
	Database = &DB{
		Host:   config.Database.Host,
		DBName: config.Database.DBName,
		Type:   config.Database.Type,
	}
	Database.getConnectionString()

	err := Database.Connect()
	if err != nil {
		log.Fatal("failed to connect to database:", err.Error())
	}
	log.Println("database connected!")
}

func (d *DB) getConnectionString() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter Database Username: ")
	username, _ := reader.ReadString('\n')

	fmt.Print("Enter Database Password: ")
	password, _ := terminal.ReadPassword(0)
	fmt.Println()

	d.Conn = strings.Replace(fmt.Sprintf("user=%s password=%s host=%s dbname=%s sslmode=disable",
		username, password,
		d.Host, d.DBName,
	), "\n", "", -1)
}

//Connect to database from config and Ping the connection
func (d *DB) Connect() error {
	db, err := sqlx.Connect(d.Type, d.Conn)
	if err != nil {
		log.Println("[Error]: DB open connection error", err.Error())
		return err
	}

	err = db.Ping()
	if err != nil {
		log.Println("[Error]: DB ping connection error", err.Error())
		return err
	}
	d.DBConnection = db

	return nil
}
