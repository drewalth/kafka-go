package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <config-file-path>\n",
			os.Args[0])
		os.Exit(1)
	}

	configFile := os.Args[1]
	conf := ReadConfig(configFile)
	conf["group.id"] = "kafka-go-getting-started"
	conf["auto.offset.reset"] = "earliest"

	c, err := kafka.NewConsumer(&conf)

	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	topic := "extraction"
	err = c.SubscribeTopics([]string{topic}, nil)
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Waiting for Kafka events...")

	// Process messages
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)

			info, infoErr := os.Stat("./tmp/")

			check(infoErr)

			fileMode := info.Mode()

			removeErr := os.RemoveAll("./tmp/")

			check(removeErr)

			recreateErr := os.MkdirAll("./tmp/", fileMode)

			check(recreateErr)

			run = false
		default:
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))

			writeFile(string(ev.Key), string(ev.Value))
		}
	}

	closeErr := c.Close()
	check(closeErr)
}

func writeFile(key string, value string) {

	file, err := os.Create(fmt.Sprintf("./tmp/%s.txt", key))
	check(err)
	defer func() {
		closeErr := file.Close()
		check(closeErr)
	}()

	_, writeErr := file.Write([]byte(value))
	check(writeErr)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
