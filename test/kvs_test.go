package test

import (
	"bytes"
	"kvs/pkg/kvs"
	"os"
	"testing"
)

func TestSimple(t *testing.T) {
	fileName := "test.db"
	db, err := kvs.New(fileName)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer os.Remove(fileName)

	key, value := []byte("key"), []byte("{ \"key\": \"value\" }")
	err = db.Write(key, value)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	newValue, err := db.Read(key)
	if err != nil {
		t.Fatalf("read failed %v", err)
	} else if !bytes.Equal(value, newValue) {
		t.Fatalf("values not equal: %v != %v", value, newValue)
	}
}

func TestSimpleReWrite(t *testing.T) {
	fileName := "test.db"
	db, err := kvs.New(fileName)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer os.Remove(fileName)

	key, value := []byte("key"), []byte("{ \"key\": \"value\" }")
	err = db.Write(key, value)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	newValue, err := db.Read(key)
	if err != nil {
		t.Fatalf("read failed %v", err)
	} else if !bytes.Equal(value, newValue) {
		t.Fatalf("values not equal: %v != %v", value, newValue)
	}

	value2 := []byte("value2")
	err = db.Write(key, value2)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	newValue, err = db.Read(key)
	if err != nil {
		t.Fatalf("read failed %v", err)
	} else if !bytes.Equal(value2, newValue) {
		t.Fatalf("values not equal: %v != %v", value, newValue)
	}
}
