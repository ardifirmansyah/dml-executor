package main

import (
	"dml-executor/src/module"
	"log"
	"time"

	"dml-executor/src"
)

func main() {
	src.ReadConfig()
	src.InitDBConnection(src.Config)

	log.Println("waiting 5 seconds for first run")
	time.Sleep(5 * time.Second)

	m := module.New(src.Config, src.Database.DBConnection)
	m.Run()
}
