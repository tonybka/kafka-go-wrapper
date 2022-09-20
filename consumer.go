package kafka_go_wrapper

import "github.com/Shopify/sarama"

type CustomConsumer struct {
	topic         string
	kafkaConsumer sarama.Consumer
}

func NewCustomConsumer(topic string, kafkaConsumer sarama.Consumer) *CustomConsumer {
	return &CustomConsumer{topic: topic, kafkaConsumer: kafkaConsumer}
}
