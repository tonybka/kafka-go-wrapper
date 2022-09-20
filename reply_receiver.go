package kafka_go_wrapper

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"time"
)

var ResponsesCache = make(map[string]chan *ResponseMessage)
var ResponseTimeout = 10 * time.Second

type ReplyReceiver struct {
	*CustomConsumer
	client sarama.Client
}

func (receiver *ReplyReceiver) Listen() error {
	log.Info("[kafka] Start listening on reply channel")
	if receiver.client == nil {
		return errors.New("kafka client is nil")
	}
	if len(receiver.topic) == 0 {
		return errors.New("invalid command topic")
	}

	partitions, err := receiver.client.Partitions(receiver.topic)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		var innerErr error
		partitionConsumer, innerErr := receiver.kafkaConsumer.ConsumePartition(
			receiver.topic,
			partition,
			sarama.OffsetOldest,
		)
		if innerErr != nil {
			return innerErr
		}
		go receiver.handePartitionConsumer(partitionConsumer)
	}

	return nil
}

// GetReplyForMessage waits for response from other external service for the request
func (receiver *ReplyReceiver) GetReplyForMessage(ctx context.Context, messageID string) (*ResponseMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, ResponseTimeout)
	defer cancel()

	select {
	case <-ctx.Done():
		delete(ResponsesCache, messageID)
		return nil, errors.New("request to service time out")
	case d := <-receiver.getReceivedByID(messageID):
		delete(ResponsesCache, messageID)
		return d, nil
	}
}

func (receiver *ReplyReceiver) handePartitionConsumer(partitionConsumer sarama.PartitionConsumer) {
	var err error
	defer func() {
		if err != nil {
			log.WithError(err).Error("[kafka] Error while handling reply")
		}
		err = partitionConsumer.Close()
		if err != nil {
			return
		}
	}()

	for message := range partitionConsumer.Messages() {
		header := msgHeaderFromKafkaRecordHeader(message.Headers)
		prevRequestMsgId := header.ReplyMsgID

		var response *ResponseMessage
		response, err = receiver.formatFromResponse(message.Value)
		if err != nil {
			return
		}
		receiver.setReceivedMessage(prevRequestMsgId, response)
	}
}

func (receiver *ReplyReceiver) formatFromResponse(d []byte) (*ResponseMessage, error) {
	var res ResponseMessage
	return &res, json.Unmarshal(d, &res)
}

// getReceivedByID return channel that using to receive reply message for particular request Message ID
func (receiver *ReplyReceiver) getReceivedByID(id string) chan *ResponseMessage {
	if response, ok := ResponsesCache[id]; ok {
		return response
	} else {
		response = make(chan *ResponseMessage)
		ResponsesCache[id] = response
		return response
	}
}

func (receiver *ReplyReceiver) setReceivedMessage(messageID string, data *ResponseMessage) {
	receiver.getReceivedByID(messageID) <- data
}
