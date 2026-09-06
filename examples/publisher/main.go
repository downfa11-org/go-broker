package main

import (
	"fmt"
	"log"
	"time"

	"github.com/cursus-io/cursus/sdk"
)

func main() {
	cfg := sdk.NewDefaultPublisherConfig()
	if err := sdk.LoadConfig("config.yaml", cfg); err != nil {
		if err := sdk.LoadConfig("../config.yaml", cfg); err != nil {
			log.Printf("Config file not found or invalid, using defaults: %v", err)
		}
	}

	p, err := sdk.NewProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			log.Printf("Error closing producer: %v", err)
		}
	}()

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("Hello Cursus! Message %d", i)
		seqNum, err := p.Send(payload)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
		} else {
			log.Printf("Sent message %d with sequence number %d", i, seqNum)
		}
		time.Sleep(1 * time.Second)
	}

	if err := p.Flush(); err != nil {
		log.Printf("flush producer: %v", err)
	}
	log.Println("Publisher finished.")
}
