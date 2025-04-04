package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	SERVICE_NAME = "KAFKA-TESTER"
)

func log(args ...string) {
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
	defer log("Application Exiting ...")
	log("Application String ...")

	osSig := make(chan os.Signal, 2)

	signal.Notify(osSig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGPIPE)

	<-osSig
}
