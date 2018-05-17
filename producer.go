package avrostry

import (
	"fmt"

	"github.com/Shopify/sarama"
)

const (
	// TODO: Create an object for manage the Kafka configuration
	kafkaConn  = "localhost:9092"
	kafkaTopic = "words"
)

//ProducerConfig Producer configuration
type ProducerConfig struct {
	ClientID      string
	MaxRetries    int
	RequiredAcks  int16
	ReturnSuccess bool
}

// EventRegistryProducer Struct for Encode and prodoce messages in Avro format with Schema Registry
type EventRegistryProducer struct {
	producer     sarama.SyncProducer
	kafkaEncoder *KafkaAvroEncoder
}

// NewEventRegistryProducer EventRegistryProducer constructor
func NewEventRegistryProducer(addrs []string, cfg *ProducerConfig) (*EventRegistryProducer, error) {
	config := sarama.NewConfig()
	config.ClientID = cfg.ClientID
	config.Producer.Retry.Max = cfg.MaxRetries
	config.Producer.RequiredAcks = sarama.RequiredAcks(cfg.RequiredAcks)
	config.Producer.Return.Successes = cfg.ReturnSuccess

	prod, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}

	//TODO Set the Kafka Schema Registry from configuration
	kafkaEncoder := NewKafkaAvroEncoder("http://127.0.0.1:8081")
	return &EventRegistryProducer{producer: prod, kafkaEncoder: kafkaEncoder}, nil
}

// Publish Encode to a Avro format and publish Domain Events to Kafka
func (erp *EventRegistryProducer) Publish(event DomainEvent) error {

	binary, err := erp.kafkaEncoder.Encode(event)

	if err != nil {
		fmt.Println(err)
		return err
	}

	msg := &sarama.ProducerMessage{
		// TODO: how we are going to manage the kafka topics?
		// TODO: Produce the messages with the AgregateID partition.
		Topic: kafkaTopic,
		Value: sarama.ByteEncoder(binary),
	}

	erp.producer.SendMessage(msg)

	return nil
}
