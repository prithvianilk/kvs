package main

import (
	"fmt"
	"kvs/pkg/kvs"
	"log"
	"os"
)

func main() {
	fileName := os.Args[1]
	kvs, err := kvs.New(fileName)
	if err != nil {
		log.Fatalf("init failed: %v", err)
	}

	for {
		var cmd string
		_, err := fmt.Scanf("%s", &cmd)
		if err != nil {
			log.Fatalf("command read failed: %v", err)
		}

		if cmd == "read" {
			var key string
			_, err := fmt.Scanf("%s", &key)
			if err != nil {
				log.Fatalf("failed to read key: %v", err)
			}
			value, err := kvs.Read([]byte(key))
			if err != nil {
				log.Printf("read failed: %v", err)
			} else {
				log.Printf("value: %v", string(value))
			}
		} else if cmd == "write" {
			var (
				key   string
				value string
			)
			_, err := fmt.Scanf("%s %s", &key, &value)
			err = kvs.Write([]byte(key), []byte(value))
			if err != nil {
				log.Printf("write failed: %v", err)
			}
		} else {
			log.Printf("invalid command: %v", cmd)
		}
	}
}
