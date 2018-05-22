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

func EventHandler(subject string, event map[string]interface{}) error {
	if subject != (Employee{}).Subject() {
		return errors.New("unknown subject")
	}
	employee := StringMapToEmployee(event)
	spew.Dump(*employee)
	fmt.Println("Consumed Employee!!")

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

	period := 30 * time.Second
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
