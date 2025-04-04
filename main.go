package main

import (
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
	useTLS          = true
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
	tlsSkipVerify = true
	useTLS = true

	osSig := make(chan os.Signal, 2)

	signal.Notify(osSig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGPIPE)

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

	t := time.Now()

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

	<-osSig
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
