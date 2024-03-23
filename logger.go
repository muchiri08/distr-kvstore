package main

import (
	"bufio"
	"fmt"
	"os"
)

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error

	ReadEvents() (<-chan Event, <-chan error)

	Run()
}

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
