package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	SERVICE_NAME = "KAFKA-TESTER"
)

var (
	bootstrapServer = ""
	topic           = ""
	certFile        = ""
	keyFile         = ""
	caFile          = ""
	tlsSkipVerify   = true
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
			InsecureSkipVerify: true,
		}
	}

	return t
}
