package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Landoop/schema-registry"
	"github.com/Shopify/sarama"
	"github.com/josgilmo/avrostry"
)

const (
	kafkaConn   = "localhost:9092"
	KAFKA_TOPIC = "words"
)

var client schemaregistry.Client

var schema = `{
	  "type": "record",
	  "name": "words",
	  "doc:": "Just words",
	  "namespace": "com.avro.kafka.golang",
	  "fields": [
		{
		  "type": "string",
		  "name": "word"
		}
	  ]
	}
	`

type WordWasRead struct {
	Word string
}

func (word WordWasRead) AvroSchema() string {
	return `{
		"type": "record",
		"name": "words",
		"doc:": "Just words",
		"namespace": "com.avro.kafka.golang",
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
	word := WordWasRead{Word: "palabrota"}

	erp := createProducer()
	erp.Publish(word)
	/*
		// var domainEvents []DomainEvent
		subjects, _ := erp.client.Subjects()
		for _, subject := range subjects {
			versions, err := client.Versions(subject)
			if err != nil {
				fmt.Println(err)
			}
			schema, err := client.GetSchemaBySubject(subject, max(versions))
			if err != nil {
				fmt.Println(err)
			}

			fmt.Println(subject, schema)
		}
	*/
}

func max(arr []int) int {
	// todo: implement
	return 1
}
