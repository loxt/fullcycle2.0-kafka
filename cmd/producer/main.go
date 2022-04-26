package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	deliveryChan := make(chan kafka.Event)

	producer := NewKafkaProducer()
	defer producer.Close()

	PublishMessage("New Message", "test", producer, nil, deliveryChan)
	go DeliveryReport(deliveryChan)
	producer.Flush(2000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "fullcycle20-kafka-kafka-1:9092",
		"delivery.timeout.ms": "5000",
		"acks":                "all",
		"enable.idempotence":  "true",
	}
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return producer
}

func PublishMessage(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for received := range deliveryChan {
		switch event := received.(type) {
		case *kafka.Message:
			if event.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v at offset %v\n", event.TopicPartition, event.TopicPartition.Offset)
			} else {
				log.Printf("Delivered message to: %v at offset %v\n", event.TopicPartition, event.TopicPartition.Offset)
			}
		}
	}
}
