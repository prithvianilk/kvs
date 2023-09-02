package main

import (
	"kvs/pkg/kvs"
	"log"
)

func main() {
	kvs, err := kvs.New("test.db")
	if err != nil {
		log.Printf("Error: %v", err)
	}

	err = kvs.Write([]byte("lol"), []byte("brrrrrrr"))
	if err != nil {
		log.Printf("Error: %v", err)
	}

	value, err := kvs.Read([]byte("lol"))
	if err != nil {
		log.Printf("error: %v", err)
	} else {
		log.Println("found entry:", string(value))
	}

	value, err = kvs.Read([]byte("ok"))
	if err != nil {
		log.Printf("error: %v", err)
	} else {
		log.Println("found entry:", string(value))
	}

	err = kvs.Write([]byte("dumb"), []byte("stupid"))
	if err != nil {
		log.Printf("Error: %v", err)
	}
}
