package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sequinstream/sequin-go"
)

func main() {
	// Parse command line flags
	token := flag.String("token", "", "Sequin API token")
	consumerGroup := flag.String("consumer-group", "", "Consumer Group name or ID")
	outputFile := flag.String("output", "", "Output file path (optional, defaults to stdout)")
	batchSize := flag.Int("batch-size", 10, "Maximum batch size for processing messages")
	baseURL := flag.String("base-url", "", "Sequin API base URL (optional, defaults to https://api.sequinstream.com/api)")
	flag.Parse()

	// Validate required flags
	if *token == "" || *consumerGroup == "" {
		log.Fatal("token and consumer-group flags are required")
	}

	// Setup output destination
	var output *os.File
	var err error
	if *outputFile != "" {
		output, err = os.OpenFile(*outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Failed to open output file: %v", err)
		}
		defer output.Close()
	} else {
		output = os.Stdout
	}

	// Initialize Sequin client
	client := sequin.NewClient(&sequin.ClientOptions{
		Token:   *token,
		BaseURL: *baseURL,
	})

	// Create message processor
	processor, err := sequin.NewProcessor(
		client,
		*consumerGroup,
		func(ctx context.Context, msgs []sequin.Message) error {
			for _, msg := range msgs {
				fmt.Fprintf(output, "%s\n", string(msg.Record))
			}
			return nil
		},
		sequin.ProcessorOptions{
			MaxBatchSize: *batchSize,
		},
	)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Run the processor
	log.Printf("Starting consumer (batch size: %d)", *batchSize)
	if err := processor.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Processor failed: %v", err)
	}
}
