package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type Message struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

const (
	SERVICE_NAME = "KAFKA-TESTER"
)

var (
	bootstrapServer = ""
	topic           = "mytopic"
	certFile        = ""
	keyFile         = ""
	caFile          = ""
	tlsSkipVerify   = false
	useTLS          = false
)

func logd(args ...string) {
	var builder strings.Builder

	for i := range args {
		builder.WriteString(args[i])

		if i < (len(args) - 1) {
			builder.WriteString(" - ")
		}
	}

	stime := time.Now().Format(time.UnixDate)

	fmt.Println("[", SERVICE_NAME, "] : [", stime, "] - ", builder.String())
}

func main() {
	defer logd("Application Exiting ...")
	logd("Application String ...")

	bootstrapServer = "my-cluster-kraft-kafka-bootstrap-kafka-tutorial-kraft-east.apps.axolab.axodevelopment.dev:443"
	topic = "mytopic"
	certFile = "user.crt"
	keyFile = "user.key"
	caFile = "ca.crt"
	tlsSkipVerify = false
	useTLS = true

	osSig := make(chan os.Signal, 2)

	signal.Notify(osSig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGPIPE)

	go func(sig chan os.Signal) {
		startProducer(sig)
	}(osSig)

	ctx, cancel := context.WithCancel(context.Background())

	go func(ctx context.Context) {
		startConsumer(ctx)
	}(ctx)

	<-osSig
	cancel()
}

func startConsumer(ctx context.Context) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_9_0_0

	if useTLS {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = createTLSConfiguration()
	}

	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	group := "my-group"

	consumer, err := sarama.NewConsumerGroup([]string{bootstrapServer}, group, config)

	if err != nil {
		log.Fatal("Failed to create consumer: %v", err)

	}

	defer consumer.Close()

	handler := consumerGroupHandler{}

	for {
		select {
		case <-ctx.Done():
			logd("Closing Application...")
			return
		default:
			err := consumer.Consume(ctx, []string{topic}, handler)
			if err != nil {
				if err == context.Canceled {
					return
				}
				log.Printf("Error from consumer: %v", err)
				time.Sleep(time.Second)

			}
		}
	}
}

type consumerGroupHandler struct{}

func (consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			log.Printf("Message received: topic=%s, partition=%d, offset=%d, key=%s, value=%s",
				message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))

			var msg Message
			if err := json.Unmarshal(message.Value, &msg); err != nil {
				log.Printf("Error unmarshaling message: %v", err)
			} else {
				log.Printf("Processed message: %+v", msg)
			}

			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func startProducer(osSig chan os.Signal) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_9_0_0

	if useTLS {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = createTLSConfiguration()
	}

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{bootstrapServer}, config)

	if err != nil {
		log.Fatal("Failed to create producer: %v", err)

	}

	defer producer.Close()

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-osSig:
			logd("Closing Application...")
			return
		case t := <-ticker.C:

			msg := Message{
				Timestamp: t,
				Message:   fmt.Sprintf("Hello Kafka at %s", t.Format(time.RFC3339)),
			}

			jmsg, err := json.Marshal(msg)

			if err != nil {
				log.Fatal("Failed to marshal json: %v", err)
			}

			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(jmsg),
			})

			if err != nil {
				log.Printf("Failed to send message: %v", err)

				//TODO: Check [Failed to send message: kafka server: The request attempted to perform an operation on an invalid topic]
			} else {
				log.Printf("Message sent to partition %d at offset %d: %s", partition, offset, string(jmsg))
			}

		}
	}
}

func createTLSConfiguration() (t *tls.Config) {
	t = &tls.Config{
		InsecureSkipVerify: tlsSkipVerify,
	}

	if certFile != "" && keyFile != "" && caFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := os.ReadFile(caFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: tlsSkipVerify,
		}
	}

	return t
}
