package module

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"dml-executor/src"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

const (
	lastIDFileName = "lastID.d"
)

type worker struct {
	db           *sqlx.DB
	logger       *logrus.Logger
	rowsAffected []int64

	tableName       string
	columnName      string
	referenceColumn string
	jobType         string
	interval        int64
	batchLimit      int64
}

func New(conf *src.Configuration, db *sqlx.DB) *worker {
	if conf.TableName == "" ||
		conf.ColumnName == "" ||
		conf.ReferenceColumn == "" {
		log.Fatal("invalid config")
	}

	return &worker{
		db:              db,
		logger:          initLog(),
		rowsAffected:    make([]int64, 0, 3),
		tableName:       conf.TableName,
		columnName:      conf.ColumnName,
		referenceColumn: conf.ReferenceColumn,
		jobType:         conf.JobType,
		interval:        conf.JobInterval,
		batchLimit:      conf.JobBatchLimit,
	}
}

func initLog() *logrus.Logger {
	pathFile := "./log/main.log"

	err := os.MkdirAll(filepath.Dir(pathFile), 0755)
	if err != nil && err != os.ErrExist {
		log.Fatal(err.Error())
	}

	f, err := os.OpenFile(pathFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err.Error())
	}

	l := logrus.New()
	l.SetOutput(f)
	l.SetFormatter(&logrus.TextFormatter{})

	return l
}

func (w *worker) TestLog() {
	w.logger.Println("hello world")
}

func (w *worker) Run() {
	// checking last id
	last := w.getLastID()

	var (
		start = last
		end   = last
	)
	w.logger.Println("starting at", start)

	for {
		var (
			err error
			rc  int64
			now = time.Now()
		)
		start, end = w.getStartEndID(last)

		logTag := fmt.Sprintf("[%d-%d]", start, end)
		log.Printf("processing: %s", logTag)

		// do job
		switch w.jobType {
		case "set_empty":
			rc, err = w.SetColumnEmptyString(start, end)
		}
		w.logger.Println(logTag, "took "+time.Since(now).String())

		if err != nil {
			w.logger.Println(logTag, err.Error())
		}

		last = end
		w.updateLastID(last)

		// validate rows affected
		if err := w.validateRowsAffected(rc); err != nil {
			log.Println("no rows affected for last 3 runs. ending process")
			break
		}

		time.Sleep(time.Minute * time.Duration(w.interval))
	}
}

// validate last 3 run rows affected
func (w *worker) validateRowsAffected(rc int64) error {
	if w.rowsAffected == nil {
		w.rowsAffected = make([]int64, 3)
		w.rowsAffected = append(w.rowsAffected, rc)
		return nil
	}

	if len(w.rowsAffected) < 3 {
		w.rowsAffected = append(w.rowsAffected, rc)
	} else {
		w.rowsAffected = w.rowsAffected[1:3]
		w.rowsAffected = append(w.rowsAffected, rc)
	}

	if len(w.rowsAffected) == 3 {
		var updated int64
		for _, v := range w.rowsAffected {
			updated += v
		}
		if updated == 0 {
			return sql.ErrNoRows
		}
	}

	return nil
}

func (w *worker) SetColumnEmptyString(start, end int64) (int64, error) {
	q := fmt.Sprintf(`UPDATE %s SET %s = '' WHERE %s BETWEEN ? AND ? AND %s <> ''`,
		w.tableName, w.columnName, w.referenceColumn, w.columnName)
	query := w.db.Rebind(q)

	rs, err := w.db.Exec(query, start, end)
	if err != nil {
		return 0, errors.New("failed to update: " + err.Error())
	}

	rc, _ := rs.RowsAffected()

	logTag := fmt.Sprintf("[%d-%d]", start, end)
	w.logger.Println(logTag, "rows affected:", rc)

	return rc, err
}

// get start end id
func (w *worker) getStartEndID(last int64) (int64, int64) {
	return last + 1, last + w.batchLimit
}

func (w *worker) getLastID() int64 {
	d, err := ioutil.ReadFile(lastIDFileName)
	if err != nil {
		log.Println("first id not found:", err.Error(), "start at 0")
		return 0
	}

	if string(d) == "" {
		return 0
	}

	dd := string(d)
	dd = strings.Replace(dd, "\n", "", -1)

	id, err := strconv.ParseInt(dd, 10, 64)
	if err != nil {
		log.Fatal(err.Error())
	}
	return id
}

func (w *worker) updateLastID(i int64) {
	f, err := os.OpenFile(lastIDFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if _, err := f.WriteString(strconv.FormatInt(i, 10)); err != nil {
		log.Fatal("failed to update last ID", err.Error())
	}
}
