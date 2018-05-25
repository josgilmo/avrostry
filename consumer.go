package avrostry

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/wvanbergen/kafka/consumergroup"
)

type ConsumerMessage struct {
	Key       []byte
	Topic     string
	Partition int32
	Offset    int64
	Subject   string
	Timestamp time.Time
	Event     map[string]interface{}
}

type EventHandler func(ConsumerMessage) error

func NullEventHandler(ConsumerMessage) error {
	return nil
}

type ErrorHandler func(error)

func NullErrorHandler(error) {
}

type consumerConfig struct {
	Zookeeper            []string
	Name                 string
	Topics               []string
	Offset               int64
	Version              sarama.KafkaVersion
	ProcessingTimeout    time.Duration
	SchemaRegistryClient SchemaRegistryClient
	CacheCodec           *CacheCodec
	EventHandler         EventHandler
	ErrorHandler         ErrorHandler
}

func DefaultKafkaRegistryConsumerGroupCfg() consumerConfig {
	return consumerConfig{
		Offset:            sarama.OffsetOldest,
		Version:           sarama.V0_10_0_0,
		ProcessingTimeout: 10 * time.Second,
		CacheCodec:        NewCacheCodec(),
		EventHandler:      NullEventHandler,
		ErrorHandler:      NullErrorHandler,
	}
}

// KafkaRegistryConsumerGroup Consumer Kafka tool with decoder.
type KafkaRegistryConsumerGroup struct {
	cg         *consumergroup.ConsumerGroup
	codec      *KafkaAvroCodec
	handler    EventHandler
	errHandler ErrorHandler
}

// NewKafkaStreamReaderRegistry Constructor for KafkaRegistryConsumerGroup
func NewKafkaStreamReaderRegistry(cfg consumerConfig) (*KafkaRegistryConsumerGroup, error) {
	config := consumergroup.NewConfig()
	config.Config.Version = cfg.Version
	config.Offsets.Initial = cfg.Offset
	config.Offsets.ProcessingTimeout = cfg.ProcessingTimeout

	// join to consumer group
	cg, err := consumergroup.JoinConsumerGroup(cfg.Name, cfg.Topics, cfg.Zookeeper, config)
	if err != nil {
		return nil, err
	}
	codec := NewKafkaAvroCodec(cfg.SchemaRegistryClient, cfg.CacheCodec)
	return &KafkaRegistryConsumerGroup{cg, codec, cfg.EventHandler, cfg.ErrorHandler}, nil
}

// ReadMessages read messages from Kafka, decode them and propagete them
// to handler, only returns when context is cancelled
func (rgc *KafkaRegistryConsumerGroup) ReadMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-rgc.cg.Errors():
			rgc.errHandler(errors.Wrap(err, "received error from kafka"))

		case msg := <-rgc.cg.Messages():
			if msg.Value == nil {
				break
			}

			var (
				eventMap map[string]interface{}
				ok       bool
			)

			subject, event, err := rgc.codec.Decode(msg.Value)
			if err != nil {
				rgc.errHandler(errors.Wrap(err, "could not decode message"))
				goto commit
			}

			eventMap, ok = event.(map[string]interface{})
			if !ok {
				rgc.errHandler(errors.Errorf("unexpected message format for subject: %s", subject))
				goto commit
			}

			err = rgc.handler(ConsumerMessage{
				Key:       msg.Key,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Subject:   subject,
				Timestamp: msg.Timestamp,
				Event:     eventMap,
			})
			if err != nil {
				break // when handler returns an error we don't commit the message
			}

		commit:
			// commit to zookeeper that message
			// this prevents read message multiple times after restart
			err = rgc.cg.CommitUpto(msg)
			if err != nil {
				rgc.errHandler(errors.Wrap(err, "could not commit message"))
			}
		}
	}
}
