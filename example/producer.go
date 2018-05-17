package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Landoop/schema-registry"
	"github.com/Shopify/sarama"
	"github.com/josgilmo/avrostry"
)

const (
	kafkaConn = "localhost:9092"
	//kafkaConn  = "localhost:7092"
	kafkaTopic = "words"
)

var client schemaregistry.Client

type wordWasRead struct {
	Word string
}

// AvroSchema for WasReadEvent
func (word wordWasRead) AvroSchema() string {
	return `{
		"type": "record",
		"name": "words",
		"doc:": "Just words",
		"namespace": "avrostry.samples.events",
		"fields": [
		{
			"type": "string",
			"name": "Word"
		}
		]
	}
	`
}

func (word wordWasRead) Version() int {
	return 1
}
func (word wordWasRead) Subject() string {
	return "ddd:words:read"
}

func (word *wordWasRead) FromPayload(m map[string]interface{}) error {
	// Take simple fields
	data, _ := json.Marshal(m)
	err := json.Unmarshal(data, word)

	return err
}

func (word *wordWasRead) ToPayload() map[string]interface{} {
	datumIn := map[string]interface{}{
		"Word": word.Word,
	}

	return datumIn
}
func (word *wordWasRead) AggregateID() interface{} {
	return "ddd:words:read"
}

func createProducer() *avrostry.EventRegistryProducer {
	// create producer
	producer, err := avrostry.NewEventRegistryProducer([]string{kafkaConn}, &avrostry.ProducerConfig{
		ClientID:      "words-producer",
		MaxRetries:    5,
		RequiredAcks:  -1,
		ReturnSuccess: true,
	})

	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	return producer
}

func main() {
	word := &wordWasRead{Word: "palabrota"}

	erp := createProducer()
	erp.Publish(word)

	fmt.Println("Finish publishing a msg")
}
