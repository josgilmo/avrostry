package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/josgilmo/avrostry"
)

const (
	zookeeperConn = "localhost:2181"
	cgroup        = "words_group"
	topic         = "words"
)

func main() {
	os.Exit(realMain())
}

func realMain() (ret int) {

	var (
		err error
	)

	kafkaConsumerManager, err := avrostry.NewKafkaStreamReaderRegistry()

	defer func() {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			ret = 1
		}
	}() // consumer config

	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	kafkaConsumerManager.ReadMessages()
	return 0
}
