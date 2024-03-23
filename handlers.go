package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

var transactionLogger TransactionLogger

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	transactionLogger.WritePut(key, string(value))
	w.WriteHeader(http.StatusCreated)
	log.Printf("PUT key=%s value=%s\n", key, string(value))
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	if errors.Is(err, ErrNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
	log.Printf("GET key=%s\n", key)
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	transactionLogger.WriteDelete(key)
	w.Write([]byte(fmt.Sprintf("value of key %s deleted successfully", key)))
	log.Printf("DELETE key=%s\n", key)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

func initializeTransactionLog() error {
	var err error

	transactionLogger, err = NewTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := transactionLogger.ReadEvents()
	count, ok, e := 0, true, Event{}

	for ok && err == nil {
		select {
		case err, ok = <-errors: //retrieving any errors
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.Key)
				count++
			case EventPut:
				err = Put(e.Key, e.Value)
				count++
			}
		}
	}
	log.Printf("%d events replayed\n", count)

	transactionLogger.Run()

	return err
}

func main() {
	err := initializeTransactionLog()
	if err != nil {
		panic(err)
	}
	mux := mux.NewRouter()
	mux.Use(loggingMiddleware)

	mux.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	mux.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	mux.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")

	log.Println("started server on port :4000")
	log.Fatal(http.ListenAndServe(":4000", mux))
}
