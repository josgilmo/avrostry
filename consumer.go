package avrostry

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

// TODO: create config for this.
const (
	zookeeperConn = "localhost:2181"
	cgroup        = "words_group"
	topic         = "words"
)

type KafkaRegistryConsumerGroup struct {
	cg           *consumergroup.ConsumerGroup
	kafkaDecoder *KafkaAvroDecoder
}

// NewKafkaStreamReaderRegistry Constructor for KafkaRegistryConsumerGroup
func NewKafkaStreamReaderRegistry() (KafkaRegistryConsumerGroup, error) {
	var (
		rcg KafkaRegistryConsumerGroup
		err error
	)
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	// join to consumer group
	// TODO: Handle error
	cg, err := consumergroup.JoinConsumerGroup(cgroup, []string{topic}, []string{zookeeperConn}, config)
	if err != nil {
		return rcg, err
	}

	// TODO: Handle error
	kafkaDecoder := NewKafkaAvroDecoder("http://127.0.0.1:8081")

	rcg = KafkaRegistryConsumerGroup{cg: cg, kafkaDecoder: kafkaDecoder}

	return rcg, nil
}

func (rgc KafkaRegistryConsumerGroup) ReadMessages() {
	// run consumer
	for {
		select {
		case msg := <-rgc.cg.Messages():
			// messages coming through chanel
			// only take messages from subscribed topic
			// TODO: Manage topics
			/*
				if msg.Topic != topic {
					continue
				}
			*/

			event, err := rgc.kafkaDecoder.Decode(msg.Value)

			fmt.Println("Topic: ", msg.Topic)
			fmt.Println("Value: ", string(msg.Value))
			fmt.Printf("Event %v: ", event)

			// commit to zookeeper that message is read
			// this prevent read message multiple times after restart
			err = rgc.cg.CommitUpto(msg)
			if err != nil {
				fmt.Println("Error commit zookeeper: ", err.Error())
			}
		}
	}
}
