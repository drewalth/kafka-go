package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"log"
	"net/http"
	"os"
)

type Payload struct {
	Key   string `form:"key"`
	Value string `form:"value"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <config-file-path>\n",
			os.Args[0])
		os.Exit(1)
	}

	router := gin.Default()

	configFile := os.Args[1]
	conf := ReadConfig(configFile)

	topic := "extraction"
	p, err := kafka.NewProducer(&conf)

	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	defer func() {
		p.Flush(15 * 1000)
		p.Close()
	}()

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	router.POST("/extraction", func(ctx *gin.Context) {

		var payload Payload

		if ctx.ShouldBind(&payload) == nil {

			productErr := p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(fmt.Sprintf("%s-%s", payload.Key, uuid.New())),
				Value:          []byte(payload.Value),
			}, nil)

			checkErr(productErr)

		} else {
			ctx.JSON(http.StatusBadRequest, "Bad request")
		}
	})

	log.Fatal(router.Run(":3000"))
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
