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

type RegistryConsumerGroup struct {
	cg           *consumergroup.ConsumerGroup
	kafkaDecoder *KafkaAvroDecoder
}

func NewKafkaStreamReaderRegistry() RegistryConsumerGroup {
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	// join to consumer group
	// TODO: Handle error
	cg, _ := consumergroup.JoinConsumerGroup(cgroup, []string{topic}, []string{zookeeperConn}, config)
	// TODO: Handle error
	kafkaDecoder := NewKafkaAvroDecoder("http://127.0.0.1:8081")
	// client, _ := schemaregistry.NewClient("http://127.0.0.1:8081")
	rcg := RegistryConsumerGroup{cg: cg, kafkaDecoder: kafkaDecoder}

	return rcg
}

/*
func EventFromConsumerMessage(msg *sarama.ConsumerMessage) DomainEvent {
	//RegisteredCodecEvents
	codec, ok := RegisteredCodecEvents[msg.Metadata.DomainName] // goavro.NewCodec(string(schemaBytes))

	datum, _, err := codec.NativeFromBinary(msg.Value)
}
*/

func (rgc RegistryConsumerGroup) ReadMessages() {
	// run consumer
	for {
		select {
		case msg := <-rgc.cg.Messages():
			// messages coming through chanel
			// only take messages from subscribed topic

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
