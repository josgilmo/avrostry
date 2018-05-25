package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
	"github.com/josgilmo/avrostry"
	"golang.org/x/sync/errgroup"

	. "github.com/josgilmo/avrostry/example/common"
)

func ErrorHandler(err error) {
	fmt.Println(err)
}

func EventHandler(msg avrostry.ConsumerMessage) error {
	if msg.Subject != (Employee{}).Subject() {
		return errors.New("unknown subject")
	}
	employee := StringMapToEmployee(msg.Event)
	spew.Dump(*employee)
	fmt.Printf("Consumed event:\n")
	fmt.Printf("\tKey: %s\n", string(msg.Key))
	fmt.Printf("\tTopic: %s\n", msg.Topic)
	fmt.Printf("\tPartition: %d\n", msg.Partition)
	fmt.Printf("\tOffset: %d\n", msg.Offset)
	fmt.Printf("\tSubject: %s\n", msg.Subject)
	fmt.Printf("\tTimestamp: %s\n", msg.Timestamp)

	return nil
}

func main() {
	cfg := avrostry.DefaultKafkaRegistryConsumerGroupCfg()
	cfg.Name = ConsumerGroup
	cfg.Topics = []string{KafkaTopic}
	cfg.Zookeeper = []string{Zookeeper}
	cfg.SchemaRegistryClient = avrostry.NewSchemaRegistryManager(
		SchemaRegistryURL,
		avrostry.NewCacheSchemaRegistry(),
		http.DefaultClient,
	)
	cfg.EventHandler = EventHandler
	cfg.ErrorHandler = ErrorHandler

	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	kafkaConsumerManager, err := avrostry.NewKafkaStreamReaderRegistry(cfg)
	if err != nil {
		panic(err)
	}

	period := 60 * time.Second
	// Run for a period
	ctx, cancel := context.WithTimeout(context.Background(), period)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return kafkaConsumerManager.ReadMessages(gctx)
	})

	fmt.Printf(">>> Consuming for %s\n", period)
	g.Wait()
	fmt.Println("Bye!!")
}
