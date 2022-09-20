package kafka_go_wrapper

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"math/rand"
	"strings"
	"time"
)

var EmptyReplyTopic = ""

type MyKafka struct {
	serviceId    string
	client       sarama.Client
	clusterAdmin sarama.ClusterAdmin

	// Currently, we do only support 1 producer
	syncProducers map[string]*CustomSyncProducer

	// Current topic message consumer
	consumers map[string]*CustomConsumer

	brokersUrl []string
}

// NewMyKafka creates new Kafka Client
// replyTopic: the topic which service will be received responses from other services
func NewMyKafka(serviceId string, hosts string, kafkaConfig KafkaConfig) (*MyKafka, error) {
	configs := formatClientConfigs(kafkaConfig)

	hostsList := strings.Split(hosts, ",")
	logrus.WithField("HostsList", hostsList).Info("[kafka] Creating new kafka client")

	client, err := sarama.NewClient(hostsList, configs)
	if err != nil {
		return nil, err
	}

	clusterAdmin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}

	consumers := make(map[string]*CustomConsumer)
	syncProducers := make(map[string]*CustomSyncProducer)

	kafka := MyKafka{
		serviceId:     serviceId,
		client:        client,
		brokersUrl:    hostsList,
		syncProducers: syncProducers,
		consumers:     consumers,
		clusterAdmin:  clusterAdmin,
	}
	return &kafka, nil
}

// CreateTopic create new topic with default config values
func (myKafka *MyKafka) CreateTopic(topicName string) error {
	existing, err := myKafka.IsTopicExisting(topicName)
	if err != nil {
		return err
	}

	if !existing {
		var creationErr error
		retention := "60000"

		topicDetail := &sarama.TopicDetail{}
		topicDetail.ConfigEntries = map[string]*string{
			"retention.ms": &retention,
		}
		topicDetail.NumPartitions = int32(1)
		topicDetail.ReplicationFactor = int16(1)

		creationErr = myKafka.clusterAdmin.CreateTopic(topicName, topicDetail, false)
		if creationErr != nil {
			return creationErr
		}
	}
	return nil
}

// CreateSyncProducerWithRepChannel create sync producer. TODO: reuse producer for the same topic
func (myKafka *MyKafka) CreateSyncProducerWithRepChannel(replyTopic string) (*CustomSyncProducer, error) {
	if myKafka.syncProducers[replyTopic] != nil {
		return myKafka.syncProducers[replyTopic], nil
	}
	producer, err := sarama.NewSyncProducerFromClient(myKafka.client)
	if err != nil {
		return nil, err
	}
	myKafka.syncProducers[replyTopic] = NewCustomSyncProducer(myKafka.serviceId, replyTopic, producer)
	return myKafka.syncProducers[replyTopic], nil
}

func (myKafka *MyKafka) CreateSyncProducer() (*CustomSyncProducer, error) {
	producer, err := sarama.NewSyncProducerFromClient(myKafka.client)
	if err != nil {
		return nil, err
	}
	return NewCustomSyncProducerWithoutReplyChannel(myKafka.serviceId, producer), nil
}

// GetReplyReceiver create single channel to receive response from other external services
// TODO: Make sure you configured corresponding producer
func (myKafka *MyKafka) GetReplyReceiver(topic string) (*ReplyReceiver, error) {
	customConsumer, err := myKafka.createConsumer(topic)
	if err != nil {
		return nil, err
	}
	receiver := &ReplyReceiver{
		CustomConsumer: customConsumer,
		client:         myKafka.client,
	}
	return receiver, nil
}

func (myKafka *MyKafka) RegisterCommandHandler(topic string) (*commandHandler, error) {
	customConsumerGroup, err := myKafka.createConsumerGroup()
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(myKafka.client)
	if err != nil {
		return nil, err
	}
	replyProducer := NewCustomSyncProducer(myKafka.serviceId, EmptyReplyTopic, producer)

	handler := &commandHandler{
		topic:              topic,
		client:             myKafka.client,
		kafkaConsumerGroup: customConsumerGroup,
		commands:           map[string]func([]byte) (interface{}, error){},
		replyProducer:      replyProducer,
		ready:              make(chan bool),
	}
	return handler, nil
}

func (myKafka *MyKafka) ListTopics() (map[string]sarama.TopicDetail, error) {
	return myKafka.clusterAdmin.ListTopics()
}

func (myKafka *MyKafka) IsTopicExisting(topic string) (bool, error) {
	var isExisting bool

	listTopics, err := myKafka.ListTopics()
	if err != nil {
		return isExisting, err
	}

	if _, ok := listTopics[topic]; ok {
		isExisting = true
	}
	return isExisting, nil
}

func (myKafka *MyKafka) Close() error {
	return myKafka.client.Close()
}

// createConsumer creates or returns the existing consumer
func (myKafka *MyKafka) createConsumer(topic string) (*CustomConsumer, error) {
	if current, ok := myKafka.consumers[topic]; ok {
		return current, nil
	}
	kafkaConsumer, err := sarama.NewConsumerFromClient(myKafka.client)
	if err != nil {
		return nil, err
	}
	customConsumer := NewCustomConsumer(topic, kafkaConsumer)

	myKafka.consumers[topic] = customConsumer
	return customConsumer, nil
}

func (myKafka *MyKafka) createConsumerGroup() (sarama.ConsumerGroup, error) {
	consumerGroupId := myKafka.serviceId // temporarily use service name is group id
	kafkaConsumerGroup, err := sarama.NewConsumerGroupFromClient(consumerGroupId, myKafka.client)
	if err != nil {
		return nil, err
	}
	return kafkaConsumerGroup, nil
}

func (myKafka *MyKafka) getRandomBroker() *sarama.Broker {
	brokers := myKafka.client.Brokers()
	rand.Seed(time.Now().Unix())
	broker := brokers[rand.Intn(len(brokers))]
	return broker
}
