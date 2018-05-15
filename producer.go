package avrostry

import (
	"fmt"

	"github.com/Shopify/sarama"
)

const (
	kafkaConn   = "localhost:9092"
	KAFKA_TOPIC = "words"
)

type ProducerConfig struct {
	ClientID      string
	MaxRetries    int
	RequiredAcks  int16
	ReturnSuccess bool
}

type EventRegistryProducer struct {
	producer     sarama.SyncProducer
	kafkaEncoder *KafkaAvroEncoder
	// clientSchemaRegistry schemaregistry.Client
}

func NewSyncProducer(addrs []string, cfg *ProducerConfig) (*EventRegistryProducer, error) {
	config := sarama.NewConfig()
	config.ClientID = cfg.ClientID
	config.Producer.Retry.Max = cfg.MaxRetries
	config.Producer.RequiredAcks = sarama.RequiredAcks(cfg.RequiredAcks)
	config.Producer.Return.Successes = cfg.ReturnSuccess

	prod, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}

	// TODO get and handle error.
	// kafkaEncoderDecoder, _ := schemaregistry.NewClient("http://127.0.0.1:8081") // schemaregistry.DefaultUrl
	kafkaEncoder := NewKafkaAvroEncoder("http://127.0.0.1:8081")
	return &EventRegistryProducer{producer: prod, kafkaEncoder: kafkaEncoder}, nil
}

type MetadataEvent struct {
	EventName string
}

func (erp *EventRegistryProducer) Publish(event DomainEvent) error {

	binary, err := erp.kafkaEncoder.Encode(event)

	if err != nil {
		fmt.Println(err)
		return err
	}

	msg := &sarama.ProducerMessage{
		// TODO: how we are going to manage the kafka topics?
		Topic: KAFKA_TOPIC,
		Value: sarama.ByteEncoder(binary),
	}

	erp.producer.SendMessage(msg)
	return nil
}
