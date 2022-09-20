package kafka_go_wrapper

import (
	"github.com/Shopify/sarama"
)

type MessageHeader struct {
	From         string `json:"from,omitempty"`
	To           string `json:"to,omitempty"`
	HandleFunc   string `json:"handleFunc,omitempty"`
	ReplyToTopic string `json:"replyToTopic,omitempty"`
	MsgID        string `json:"msgId,omitempty"`
	ReplyMsgID   string `json:"replyMsgId,omitempty"` // only available in response
}

func (msgHeader MessageHeader) Format() []sarama.RecordHeader {
	var headers []sarama.RecordHeader

	if msgHeader.From != "" {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte("from"),
			Value: []byte(msgHeader.From),
		})
	}

	if msgHeader.To != "" {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte("to"),
			Value: []byte(msgHeader.To),
		})
	}

	if msgHeader.HandleFunc != "" {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte("handleFunc"),
			Value: []byte(msgHeader.HandleFunc),
		})
	}

	if msgHeader.ReplyToTopic != "" {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte("replyToTopic"),
			Value: []byte(msgHeader.ReplyToTopic),
		})
	}

	if msgHeader.MsgID != "" {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte("msgId"),
			Value: []byte(msgHeader.MsgID),
		})
	}

	if msgHeader.ReplyMsgID != "" {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte("replyMsgId"),
			Value: []byte(msgHeader.ReplyMsgID),
		})
	}
	return headers
}

func msgHeaderFromKafkaRecordHeader(recordHeaders []*sarama.RecordHeader) MessageHeader {
	var header MessageHeader

	for _, h := range recordHeaders {
		switch string(h.Key) {
		case "from":
			header.From = string(h.Value)
		case "to":
			header.To = string(h.Value)
		case "handleFunc":
			header.HandleFunc = string(h.Value)
		case "replyToTopic":
			header.ReplyToTopic = string(h.Value)
		case "msgId":
			header.MsgID = string(h.Value)
		case "replyMsgId":
			header.ReplyMsgID = string(h.Value)
		default:
		}
	}
	return header
}
