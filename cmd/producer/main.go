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

	event := <-deliveryChan
	msg := event.(*kafka.Message)
	if msg.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v\n", msg.TopicPartition.Error)
	} else {
		log.Printf("Delivered message to topic %s [%d] at offset %v\n",
			msg.TopicPartition, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
	}

	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "fullcycle20-kafka-kafka-1:9092",
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
