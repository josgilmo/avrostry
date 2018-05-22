package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
	"github.com/josgilmo/avrostry"

	. "github.com/josgilmo/avrostry/example/common"
)

func main() {
	cfg := avrostry.DefaultProducerConfig()
	cfg.Addrs = []string{KafkaAddr}
	cfg.ClientID = ClientID
	cfg.SchemaRegistryClient = avrostry.NewSchemaRegistryManager(
		SchemaRegistryURL,
		avrostry.NewCacheSchemaRegistry(),
		http.DefaultClient)

	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	// create producer
	producer, err := avrostry.NewKafkaRegistryProducer(cfg)
	if err != nil {
		panic(err)
	}

	employee := Employee{
		EmployeeID: fmt.Sprintf("%d", time.Now().Unix()),
		FirstName:  "John",
		LastName:   "Doe",
		Age:        51,
		Emails:     []string{"john.doe@example.org", "john.doe@example.com"},
		Phone: Phone{
			CountryCode: "44",
			Number:      "2070685000",
		},
		Status: Hourly,
	}

	err = producer.Publish(KafkaTopic, &employee)
	if err != nil {
		panic(err)
	}

	spew.Dump(employee)

	fmt.Println("Published employee!!")
}
