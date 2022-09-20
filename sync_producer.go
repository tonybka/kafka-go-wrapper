package kafka_go_wrapper

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type CustomSyncProducer struct {
	serviceId  string
	replyTopic string
	producer   sarama.SyncProducer
}

func NewCustomSyncProducer(
	serviceId string,
	replyTopic string,
	producer sarama.SyncProducer,
) *CustomSyncProducer {
	return &CustomSyncProducer{serviceId: serviceId, producer: producer, replyTopic: replyTopic}
}

func NewCustomSyncProducerWithoutReplyChannel(
	serviceId string,
	producer sarama.SyncProducer,
) *CustomSyncProducer {
	return &CustomSyncProducer{serviceId: serviceId, producer: producer}
}

func (prod *CustomSyncProducer) SendMessageWithID(topic, messageID, handleFunc string, message []byte) error {
	return prod.sendMessage(topic, messageID, handleFunc, message)
}

// SendMessage send new message with synchronous publisher
func (prod *CustomSyncProducer) SendMessage(topic string, handleFunc string, message []byte) error {
	messageUID := uuid.New().String()
	return prod.sendMessage(topic, messageUID, handleFunc, message)
}

// reply responds to ongoing request from external service
func (prod *CustomSyncProducer) reply(replyTopic, reqMessageID string, value []byte) error {
	logrus.WithFields(logrus.Fields{"ReqMessageID": reqMessageID}).Info("[kafka] Responding message")
	messageHeader := MessageHeader{
		From:       prod.serviceId,
		To:         replyTopic,
		ReplyMsgID: reqMessageID,
	}
	if prod.producer == nil {
		return errors.New("kafka producer is nil")
	}

	_, _, err := prod.producer.SendMessage(&sarama.ProducerMessage{
		Topic:   replyTopic,
		Value:   sarama.ByteEncoder(value),
		Headers: messageHeader.Format(),
	})
	if err != nil {
		return err
	}
	return nil
}

func (prod *CustomSyncProducer) sendMessage(topic, messageID, handleFunc string, message []byte) error {
	//logrus.WithFields(logrus.Fields{"topic": topic, "messageID": messageID, "handleFunc": handleFunc}).
	//	Info("[kafka] Sending event")

	messageHeader := MessageHeader{
		MsgID:        messageID,
		From:         prod.serviceId,
		To:           topic,
		ReplyToTopic: prod.replyTopic,
		HandleFunc:   handleFunc,
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(message),
		Headers: messageHeader.Format(),
	}
	partition, offset, err := prod.producer.SendMessage(msg)
	if err != nil {
		return err
	}
	logrus.WithFields(logrus.Fields{"partition": partition, "offset": offset}).Info("[kafka] Published new event")
	return nil
}
