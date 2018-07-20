package avrostry

import (
	"context"
	"fmt"
	"math/rand"
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
	Headers   []MessageHeader
	Event     map[string]interface{}
}

func (cm *ConsumerMessage) GetFieldValuesFromEvent(fieldsToRetreive map[string]interface{}) error {

	var (
		ok bool
	)

	for fieldName, target := range fieldsToRetreive {

		field, found := cm.Event[fieldName]
		if !found {
			return errors.New("The event content is not valid")
		}
		switch t := target.(type) {
		case *string:
			*(target).(*string), ok = field.(string)
		case *[]string:
			*(target).(*[]string), ok = field.([]string)
		case *[]byte:
			*(target).(*[]byte), ok = field.([]byte)
		case *bool:
			*(target).(*bool), ok = field.(bool)
		case *int:
			*(target).(*int), ok = field.(int)
		case *int16:
			*(target).(*int16), ok = field.(int16)
		case *int32:
			*(target).(*int32), ok = field.(int32)
		case *int64:
			*(target).(*int64), ok = field.(int64)
		case *uint:
			*(target).(*uint), ok = field.(uint)
		case *uint16:
			*(target).(*uint16), ok = field.(uint16)
		case *uint32:
			*(target).(*uint32), ok = field.(uint32)
		case *uint64:
			*(target).(*uint64), ok = field.(uint64)
		case *float32:
			*(target).(*float32), ok = field.(float32)
		case *float64:
			*(target).(*float64), ok = field.(float64)
		default:
			return errors.Errorf("Event field type %T not supported", t)
		}
		if !ok {
			return errors.New("The event field type is not valid")
		}
	}

	return nil
}

type DiscardedMessageError struct {
	msg *ConsumerMessage
}

func (e *DiscardedMessageError) Error() string {
	return fmt.Sprintf("discarded message: key: %s, topic: %s, partition: %d, offset: %d", string(e.msg.Key), e.msg.Topic, e.msg.Partition, e.msg.Offset)
}

func (e *DiscardedMessageError) Message() *ConsumerMessage {
	return e.msg
}

type EventHandler func(*ConsumerMessage) (shouldCommit bool)

func NullEventHandler(*ConsumerMessage) bool {
	return true
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
	// Backoff config
	MaxRetries         int // 0 for infinite retries
	MaxIntervalSeconds int // max seconds to sleep between retries
}

func DefaultKafkaRegistryConsumerGroupCfg() consumerConfig {
	return consumerConfig{
		Offset:             sarama.OffsetOldest,
		Version:            sarama.V0_11_0_0,
		ProcessingTimeout:  10 * time.Second,
		CacheCodec:         NewCacheCodec(),
		EventHandler:       NullEventHandler,
		ErrorHandler:       NullErrorHandler,
		MaxRetries:         0,
		MaxIntervalSeconds: 30,
	}
}

// KafkaRegistryConsumerGroup Consumer Kafka tool with decoder.
type KafkaRegistryConsumerGroup struct {
	cfg        consumerConfig
	cg         *consumergroup.ConsumerGroup
	codec      *KafkaAvroCodec
	random     *rand.Rand
	handler    EventHandler
	errHandler ErrorHandler
}

// NewKafkaStreamReaderRegistry Constructor for KafkaRegistryConsumerGroup
func NewKafkaStreamReaderRegistry(cfg consumerConfig) (*KafkaRegistryConsumerGroup, error) {
	config := consumergroup.NewConfig()
	config.Config.Version = cfg.Version
	config.Offsets.Initial = cfg.Offset
	config.Offsets.ProcessingTimeout = cfg.ProcessingTimeout
	config.Consumer.Return.Errors = true

	// join to consumer group
	cg, err := consumergroup.JoinConsumerGroup(cfg.Name, cfg.Topics, cfg.Zookeeper, config)
	if err != nil {
		return nil, err
	}
	codec := NewKafkaAvroCodec(cfg.SchemaRegistryClient, cfg.CacheCodec)
	return &KafkaRegistryConsumerGroup{
		cfg:        cfg,
		cg:         cg,
		codec:      codec,
		random:     rand.New(rand.NewSource(time.Now().UnixNano())),
		handler:    cfg.EventHandler,
		errHandler: cfg.ErrorHandler}, nil
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
				eventMap       map[string]interface{}
				ok             bool
				consumerMsg    *ConsumerMessage
				retry          int
				backoff        float64
				messageHeaders []MessageHeader
			)

			// Taking message headers
			if len(msg.Headers) > 0 {
				messageHeaders = make([]MessageHeader, len(msg.Headers))
				for z, h := range msg.Headers {
					messageHeaders[z] = MessageHeader{
						Key:   string(h.Key),
						Value: string(h.Value),
					}
				}
			}

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

			consumerMsg = &ConsumerMessage{
				Key:       msg.Key,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Subject:   subject,
				Timestamp: msg.Timestamp,
				Event:     eventMap,
				Headers:   messageHeaders,
			}

			for {
				if rgc.cfg.MaxRetries > 0 && retry >= rgc.cfg.MaxRetries {
					rgc.errHandler(&DiscardedMessageError{consumerMsg})
					break // max num of retries reached, commit message anyway
				}

				// As long as handler returns false we retry the same message.
				// Clients must know in which situation messages cannnot be commited
				// and should stop the consumption loop.

				shouldCommit := rgc.handler(consumerMsg)
				if shouldCommit {
					break
				}

				retry++
				backoff = float64(uint(1) << uint(retry))         // 2 ^ retry
				backoff += backoff * (0.1 * rgc.random.Float64()) // add a maximum of 10%
				if backoff > float64(rgc.cfg.MaxIntervalSeconds) {
					backoff = float64(rgc.cfg.MaxIntervalSeconds)
				}

				select {
				case <-time.After(time.Second * time.Duration(backoff)):
					break
				case <-ctx.Done():
					return nil
				}
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
