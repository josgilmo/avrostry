package avrostry

import (
	"github.com/Shopify/sarama"
)

// ProducerConfig Producer configuration
type producerConfig struct {
	Addrs         []string
	ClientID      string
	MaxRetries    int
	RequiredAcks  sarama.RequiredAcks
	ReturnSuccess bool
	Compression   sarama.CompressionCodec
	Version       sarama.KafkaVersion
	//
	SchemaRegistryClient SchemaRegistryClient
	CacheCodec           *CacheCodec
}

func DefaultProducerConfig() producerConfig {
	return producerConfig{
		MaxRetries:    5,
		RequiredAcks:  sarama.WaitForLocal,
		ReturnSuccess: true,
		Compression:   sarama.CompressionSnappy,
		Version:       sarama.V0_10_0_0,
		CacheCodec:    NewCacheCodec(),
	}
}

// KafkaRegistryProducer Struct for Encode and publish messages in Avro format with Schema Registry
type KafkaRegistryProducer struct {
	producer sarama.SyncProducer
	codec    *KafkaAvroCodec
}

func NewKafkaRegistryProducer(cfg producerConfig) (*KafkaRegistryProducer, error) {
	config := sarama.NewConfig()
	config.ClientID = cfg.ClientID
	config.Producer.Retry.Max = cfg.MaxRetries
	config.Producer.RequiredAcks = cfg.RequiredAcks
	config.Producer.Return.Successes = cfg.ReturnSuccess
	config.Producer.Compression = sarama.CompressionSnappy
	config.Version = cfg.Version

	producer, err := sarama.NewSyncProducer(cfg.Addrs, config)
	if err != nil {
		return nil, err
	}
	codec := NewKafkaAvroCodec(cfg.SchemaRegistryClient, cfg.CacheCodec)
	return &KafkaRegistryProducer{producer, codec}, nil
}

// Publish encode to a Avro format and publish a DomainEvent to Kafka
func (erp *KafkaRegistryProducer) Publish(topic string, event DomainEvent) (partition int32, offset int64, err error) {
	binary, err := erp.codec.Encode(event)
	if err != nil {
		return -1, -1, err
	}

	msg := &sarama.ProducerMessage{
		Key:   sarama.StringEncoder(event.ID()),
		Topic: topic,
		Value: sarama.ByteEncoder(binary),
	}

	return erp.producer.SendMessage(msg)
}
