package main

import (
	"errors"
	"testing"
)

func TestPut(t *testing.T) {
	const key = "create-key"
	const value = "create-value"

	var val interface{}
	var contains bool

	defer delete(store.m, key)

	// sanity check
	_, contains = store.m[key]
	if contains {
		t.Error("key/value already exists")
	}

	// err should be nil
	err := Put(key, value)
	if err != nil {
		t.Error(err)
	}

	val, contains = store.m[key]
	if !contains {
		t.Error("create failed")
	}

	if val != value {
		t.Error("val/value missmatch")
	}
}

func TestGet(t *testing.T) {
	const key = "read-key"
	const value = "read-value"

	var val interface{}
	var err error

	defer delete(store.m, key)

	// read non existing value
	val, err = Get(key)
	if err == nil {
		t.Error("expected an error")
	}
	if !errors.Is(err, ErrNoSuchKey) {
		t.Error("unexpected error: ", err)
	}

	store.m[key] = value

	// reading the value
	val, err = Get(key)
	if err != nil {
		t.Error("unexpected error: ", err)
	}

	if val != value {
		t.Error("val/value missmatch")
	}
}

func TestDelete(t *testing.T) {
	var key = "delete-key"
	var value = "delete-value"

	var contains bool

	defer delete(store.m, key)

	store.m[key] = value

	_, contains = store.m[key]
	if !contains {
		t.Error("key/value doesn't exist")
	}

	Delete(key)

	_, contains = store.m[key]
	if contains {
		t.Error("delete failed")
	}
}
