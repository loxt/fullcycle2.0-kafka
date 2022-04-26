package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	configMap := kafka.ConfigMap{
		"bootstrap.servers": "fullcycle20-kafka-kafka-1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
		"auto.offset.reset": "earliest",
	}
	consumer, error := kafka.NewConsumer(&configMap)
	if error != nil {
		fmt.Println("Error creating consumer: ", error.Error())
	}
	topics := []string{"test"}
	consumer.SubscribeTopics(topics, nil)
	for {
		msg, error := consumer.ReadMessage(-1)
		if error != nil {
			fmt.Println("Error reading message: ", error.Error())
		}
		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	}
}
