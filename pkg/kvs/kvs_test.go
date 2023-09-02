package kvs

import (
	"bytes"
	"os"
	"testing"
)

func TestSimple(t *testing.T) {
	fileName := "test.db"
	db, err := New(fileName)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer db.Close()
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

func TestSimpleRewrite(t *testing.T) {
	fileName := "test.db"
	db, err := New(fileName)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer db.Close()
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

func TestSimpleDelete(t *testing.T) {
	fileName := "test.db"
	db, err := New(fileName)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer db.Close()
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

	err = db.Delete(key)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	newValue, err = db.Read(key)
	if err == nil {
		t.Fatalf("read succeeded, expected it to fail")
	} else if err != EntryNotFound {
		t.Fatalf("read returned wrong error: %v", err)
	} else if newValue != nil {
		t.Fatalf("read failed but returned value: %v", value)
	}
}
