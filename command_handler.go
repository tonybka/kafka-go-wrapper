package kafka_go_wrapper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

var ErrorCommandHandlerNotFound = errors.New("command handler not found")

type commandHandler struct {
	topic              string
	client             sarama.Client
	kafkaConsumerGroup sarama.ConsumerGroup
	replyProducer      *CustomSyncProducer

	// Mapping command -> handler function
	commands map[string]func([]byte) (interface{}, error)
	ready    chan bool
}

// RegisterCommand register handler of specific topic
func (commander *commandHandler) RegisterCommand(name string, handle func([]byte) (interface{}, error)) {
	log.WithField("Command", name).Info("[kafka] Register request command")
	if _, ok := commander.commands[name]; ok {
		panic(fmt.Errorf("[%v] handler is already exsit", name))
	}
	commander.commands[name] = handle
}

// Listen start listening for upcoming commands
func (commander *commandHandler) Listen() error {
	log.WithField("topic", commander.topic).Info("[kafka] Start listening for upcoming commands via kafka")
	if commander.client == nil {
		return errors.New("kafka client is nil")
	}
	if len(commander.topic) == 0 {
		return errors.New("invalid command topic")
	}

	keepRunning := true
	ctx, cancel := context.WithCancel(context.Background())
	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side re-balance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := commander.kafkaConsumerGroup.Consume(
				ctx, strings.Split(commander.topic, ","), commander); err != nil {
				log.Panicf("[kafka] Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			commander.ready = make(chan bool)
		}
	}()

	<-commander.ready // Wait till the consumer has been set up
	log.Println("[kafka] Sarama consumer up and running.")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("[kafka] Terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("[kafka] Terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			commander.toggleConsumptionFlow(commander.kafkaConsumerGroup, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()
	if err := commander.client.Close(); err != nil {
		log.Panicf("[kafka] Error closing client: %v", err)
	}

	return nil
}

func (commander *commandHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(commander.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (commander *commandHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (commander *commandHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			log.Infof("[kafka] Received: message = %s, timestamp = %v", string(message.Value), message.Timestamp)
			commander.handleMessage(message)
			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			log.Info("[kafka] Session is done")
			return nil
		}
	}
}

func (commander *commandHandler) toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("[kafka] Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("[kafka] Pausing consumption")
	}
	*isPaused = !*isPaused
}

// handleMessage parse message component, proceed the command and respond
func (commander *commandHandler) handleMessage(req *sarama.ConsumerMessage) {
	header := msgHeaderFromKafkaRecordHeader(req.Headers)
	requestMsgId := header.MsgID
	// log.WithField("MessageID", requestMsgId).Info("[kafka] Handling event")

	// find in mapping of handler function
	if handlerFunc, ok := commander.commands[header.HandleFunc]; ok {
		if header.ReplyToTopic == "" {
			// if it is the function that does not require responding
			_, err := handlerFunc(req.Value)
			if err != nil {
				return
			}
		} else {
			var err error
			response := ResponseMessage{}

			handleReturn, err := handlerFunc(req.Value)
			if err != nil {
				response.Error = err.Error()
			} else {
				var resData []byte
				resData, err = json.Marshal(handleReturn)
				if err == nil {
					response.Data = resData
				} else {
					response.Error = err.Error()
				}
			}

			if err = commander.replyProducer.reply(header.ReplyToTopic, requestMsgId, response.Bytes()); err != nil {
				errMsg := fmt.Sprintf("Reply to %v error: %v", header.ReplyToTopic, err)
				log.Errorf(errMsg)
			}
		}
		return
	} else {
		// No handler function
		response := ResponseMessage{
			Error: ErrorCommandHandlerNotFound.Error(),
		}
		if err := commander.replyProducer.reply(header.ReplyToTopic, requestMsgId, response.Bytes()); err != nil {
			errMsg := fmt.Sprintf("reply to %v error: %v", header.ReplyToTopic, err)
			log.Errorf(errMsg)
		}
	}
}
