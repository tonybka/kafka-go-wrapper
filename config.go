package kafka_go_wrapper

import (
	"github.com/Shopify/sarama"
)

type KafkaConfig struct {
	KafkaProducerRetries int
}

// formatClientConfigs setup default configuration values
func formatClientConfigs(kafkaConfig KafkaConfig) *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_7_0_0

	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = kafkaConfig.KafkaProducerRetries

	// Consumption: disable and manually submit
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	return config
}
