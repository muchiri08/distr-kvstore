package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error

	ReadEvents() (<-chan Event, <-chan error)

	Run()
}

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

// File Transaction Logger Implementation

type FileTransactionLogger struct {
	events       chan<- Event // write only channel for sending events
	errors       <-chan error // read-only channel for receiving errors
	lastSequence uint64       // last used event sequence number
	file         *os.File     // location of transaction log
}

func NewTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}

	return &FileTransactionLogger{file: file}, nil
}

func (ftl *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	ftl.events = events

	errors := make(chan error, 1)
	ftl.errors = errors

	go func() {
		for e := range events {
			ftl.lastSequence++
			_, err := fmt.Fprintf(ftl.file, "%d\t%d\t%s\t%s\n", ftl.lastSequence, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
				return
			}
			// map operations
			switch e.EventType {
			case EventDelete:
				Delete(e.Key)
			case EventPut:
				Put(e.Key, e.Value)
			}
		}
	}()
}

func (ftl *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(ftl.file)
	outEvent := make(chan Event)    // unbuffered event channel
	outError := make(chan error, 1) // buffered error channel

	go func() {
		var e Event
		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}
			// Sanity check! Are the sequence numbers in increasing order?
			if ftl.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}

			ftl.lastSequence = e.Sequence // Update last used sequence
			outEvent <- e
		}

		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()

	return outEvent, outError
}

func (ftl *FileTransactionLogger) WritePut(key, value string) {
	ftl.events <- Event{EventType: EventPut, Key: key, Value: value}

}

func (ftl *FileTransactionLogger) WriteDelete(key string) {
	ftl.events <- Event{EventType: EventDelete, Key: key}

}

func (ftl *FileTransactionLogger) Err() <-chan error {
	return ftl.errors
}

// Postgres Transaction Logger Implementation

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
}

type PostgresDBParams struct {
	dbName   string
	host     string
	user     string
	password string
}

func NewPostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {
	var connectionString = fmt.Sprintf("host=%s dbname=%s user=%s password=%s", config.host, config.dbName, config.user, config.password)
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	ptl := &PostgresTransactionLogger{db: db}
	exists, _ := ptl.verifyTableExists()
	if !exists {
		if err = ptl.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return ptl, nil
}

func (ptl *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	query := `SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname='public' AND tablename = 'transactions');`
	var exists bool

	err := ptl.db.QueryRow(query).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("something went wrong while verifying table existsence")
	}
	if !exists {
		return false, fmt.Errorf("table transactions does not exists")
	}
	return exists, nil
}

func (ptl *PostgresTransactionLogger) createTable() error {
	query := `CREATE TABLE transactions(
		sequence SERIAL PRIMARY KEY, 
		event_type SMALLINT NOT NULL, key VARCHAR(255), value VARCHAR(255));`

	_, err := ptl.db.Exec(query)
	return err
}

func (ptl *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	ptl.events = events

	errors := make(chan error, 1)
	ptl.errors = errors

	go func() {
		query := `INSERT INTO transactions (event_type, key, value) VALUES ($1, $2, $3)`

		for e := range events {
			_, err := ptl.db.Exec(query, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
			}
			// map operations
			switch e.EventType {
			case EventDelete:
				Delete(e.Key)
			case EventPut:
				Put(e.Key, e.Value)
			}
		}
	}()
}

func (ptl *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		defer close(outEvent)
		defer close(outError)

		query := `SELECT sequence, event_type, key, value FROM transactions ORDER BY sequence`

		rows, err := ptl.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}

		defer rows.Close()

		e := Event{}

		for rows.Next() {
			err = rows.Scan(&e.Sequence, &e.EventType, &e.Key, &e.Value)
			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}

			outEvent <- e
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()

	return outEvent, outError
}

func (ptl *PostgresTransactionLogger) WriteDelete(key string) {
	ptl.events <- Event{EventType: EventDelete, Key: key}
}

func (ptl *PostgresTransactionLogger) WritePut(key, value string) {
	ptl.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (ptl *PostgresTransactionLogger) Err() <-chan error {
	return ptl.errors
}
