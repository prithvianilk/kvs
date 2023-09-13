package main

import (
	"flag"
	"fmt"
	"kvs/pkg/kvs"
	"kvs/pkg/kvs/config"
	"log"
	"os"
)

func main() {
	cfg := getConfig()
	db, err := kvs.New(cfg)
	if err != nil {
		log.Fatalf("init failed: %v", err)
	}
	defer db.Close()

	for {
		var cmd string
		_, err := fmt.Scanf("%s", &cmd)
		if err != nil {
			log.Printf("command read failed: %v", err)
			continue
		}

		if cmd == "read" {
			var key string
			_, err := fmt.Scanf("%s", &key)
			if err != nil {
				log.Fatalf("failed to read key: %v", err)
			}
			value, err := db.Read([]byte(key))
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
			err = db.Write([]byte(key), []byte(value))
			if err != nil {
				log.Printf("write failed: %v", err)
			}
		} else if cmd == "delete" {
			var key string
			_, err := fmt.Scanf("%s", &key)
			if err != nil {
				log.Fatalf("failed to read key: %v", err)
			}
			err = db.Delete([]byte(key))
			if err != nil {
				log.Printf("delete failed: %v", err)
			}
		} else {
			log.Printf("invalid command: %v", cmd)
		}
	}
}

func getConfig() *config.Config {
	cfg := &config.Config{}
	flag.IntVar(
		&cfg.LogFileSizeThresholdInBytes,
		"max-log-size",
		config.DefaultLogFileSizeThresholdInBytes,
		"Log file size threshold in bytes",
	)
	flag.Int64Var(
		&cfg.CompactionWorkerSleepTimeInMillis,
		"compaction-sleep-time",
		config.DefaultCompactionWorkerSleepTimeInMillis,
		"Log file compaction worker sleep time in millis",
	)
	flag.Parse()
	dbName := os.Args[len(os.Args)-1]
	cfg.DbName = dbName
	return cfg
}
