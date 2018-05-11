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
	kafkaConn  = "localhost:9092"
	KafkaTopic = "words"
)

var client schemaregistry.Client

// WordWasRead Sample event
type WordWasRead struct {
	Word string
}

// AvroSchema for WasReadEvent
func (word WordWasRead) AvroSchema() string {
	return `{
		"type": "record",
		"name": "words",
		"doc:": "Just words",
		"namespace": "avrostry.samples.events",
		"fields": [
		{
			"type": "string",
			"name": "word"
		}
		]
	}
	`
}

func (word WordWasRead) Version() int {
	return 1
}
func (word WordWasRead) Subject() string {
	return "ddd:words:read"
}

func (word *WordWasRead) FromPayload(m map[string]interface{}) error {
	// Take simple fields
	data, _ := json.Marshal(m)
	err := json.Unmarshal(data, word)

	return err
}

func (word *WordWasRead) ToPayload() map[string]interface{} {
	datumIn := map[string]interface{}{
		"Word": word.Word,
	}

	return datumIn
}

func createProducer() *avrostry.EventRegistryProducer {
	// create producer
	producer, err := avrostry.NewSyncProducer([]string{kafkaConn}, &avrostry.ProducerConfig{
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
	word := &WordWasRead{Word: "palabrota"}

	erp := createProducer()
	erp.Publish(word)

}
