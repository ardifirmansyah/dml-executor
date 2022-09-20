package src

import (
	"encoding/json"
	"io/ioutil"
	"log"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type Configuration struct {
	Database      DB    `json:"database"`
	JobBatchLimit int64 `json:"job_batch_limit"`
	JobInterval   int64 `json:"job_interval"`

	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
	JobType    string `json:"job_type"`
}

var Config *Configuration

func ReadConfig() {
	Config = &Configuration{}

	if !ReadModuleJSONConfig(Config, "./config", "main") {
		log.Fatal("failed to read config")
	}

	b, _ := json.Marshal(Config)
	log.Println("success reading config:", string(b))
}

func ReadModuleJSONConfig(cfg interface{}, path, module string) bool {
	filename := path + "/" + module + ".json"

	b, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Println(err.Error())
		return false
	}

	err = json.Unmarshal(b, cfg)
	if err != nil {
		log.Println(err.Error())
		return false
	}

	return true
}
