package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sequinstream/sequin-go"
	"github.com/sequinstream/sequin-go/examples/audit_logging/internal/db"
	"github.com/sequinstream/sequin-go/examples/audit_logging/internal/upserter"
)

func main() {
	// Parse command line flags
	token := flag.String("token", "", "Sequin API token")
	baseURL := flag.String("base-url", "", "Sequin API base URL (optional)")
	dbHost := flag.String("db-host", "localhost", "Database host")
	dbPort := flag.Int("db-port", 5432, "Database port")
	dbUser := flag.String("db-user", "", "Database user")
	dbPass := flag.String("db-password", "", "Database password")
	dbName := flag.String("db-name", "", "Database name")
	batchSize := flag.Int("batch-size", 100, "Maximum batch size for processing messages")
	userPermConsumer := flag.String("user-perm-consumer", "user-permissions-consumer", "Consumer group for user permissions")
	subsConsumer := flag.String("subs-consumer", "subscriptions-consumer", "Consumer group for subscriptions")
	flag.Parse()

	// Validate required flags
	if *token == "" || *dbUser == "" || *dbPass == "" || *dbName == "" {
		log.Fatal("token, db-user, db-password, and db-name flags are required")
	}

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to database
	dbPool, err := db.Connect(ctx, db.Config{
		Host:     *dbHost,
		Port:     *dbPort,
		User:     *dbUser,
		Password: *dbPass,
		Database: *dbName,
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer dbPool.Close()

	// Initialize upserter with consumer group names
	ups := upserter.New(dbPool, *userPermConsumer, *subsConsumer)

	// Initialize Sequin client with baseURL
	clientOpts := &sequin.ClientOptions{
		Token: *token,
	}
	if *baseURL != "" {
		clientOpts.BaseURL = *baseURL
	}
	client := sequin.NewClient(clientOpts)

	// Initialize slice to hold all our Sequin processors (one per table)
	processors := make([]*sequin.Processor, 0)

	// Iterate through each table configuration we defined in the upserter
	for _, cfg := range ups.GetConfigs() {
		// Create a new Sequin processor for this table
		processor, err := sequin.NewProcessor(
			client,
			cfg.ConsumerGroup, // Each table has its own consumer group
			// This function is called by Sequin when new messages arrive
			func(ctx context.Context, msgs []sequin.Message) error {
				log.Printf("Received batch of %d messages", len(msgs))

				// Pre-allocate slice to hold all events in this batch
				events := make([]upserter.AuditEvent, len(msgs))

				// Convert each Sequin message into our AuditEvent type
				for i, msg := range msgs {
					var event upserter.AuditEvent
					// Parse the JSON message into our struct
					if err := json.Unmarshal(msg.Record, &event); err != nil {
						log.Printf("Error unmarshaling message %d: %v", i, err)
						return fmt.Errorf("unmarshaling message %d: %w", i, err)
					}
					events[i] = event
				}

				// Process all events in this batch for the specific table
				log.Printf("Processing %d events for table %s", len(events), events[0].TableName)
				if err := ups.ProcessTableEvents(ctx, events); err != nil {
					log.Printf("Error processing events: %v", err)
					return err
				}

				log.Printf("Successfully processed %d events", len(events))
				return nil
			},
			sequin.ProcessorOptions{
				MaxBatchSize: *batchSize, // Control how many messages to process at once
			},
		)
		if err != nil {
			log.Fatalf("Failed to create processor for %s: %v", cfg.TableName, err)
		}

		// Store the processor so we can start them all later
		processors = append(processors, processor)
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Run all processors
	log.Printf("Starting audit processors (batch size: %d)", *batchSize)
	errChan := make(chan error, len(processors))
	for _, proc := range processors {
		go func(p *sequin.Processor) {
			if err := p.Run(ctx); err != nil && err != context.Canceled {
				errChan <- fmt.Errorf("processor failed: %w", err)
			}
		}(proc)
	}

	// Wait for either context cancellation or an error
	select {
	case <-ctx.Done():
		if ctx.Err() != context.Canceled {
			log.Fatalf("Context error: %v", ctx.Err())
		}
	case err := <-errChan:
		log.Fatal(err)
	}
}
