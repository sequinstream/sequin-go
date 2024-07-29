# `sequin-go`

A lightweight Go SDK for sending, receiving, and acknowledging messages in [Sequin streams](https://github.com/sequinstream/sequin). For easy development and testing, it also comes with helpful methods for managing the lifecycle of streams and consumers.

## Installing

Install the library:

```shell
go get github.com/sequinstream/sequin-go
```

## Initializing

You'll typically initialize a Sequin `Client` once in your application. Create a new file to initialize the `Client` in, and use it in other parts of your app:

```go
package sequin

import (
    "os"
    "github.com/sequinstream/sequin-go"
)

var Client *sequin.Client

func init() {
    baseURL := os.Getenv("SEQUIN_URL")
    if baseURL == "" {
        baseURL = "http://localhost:7376"
    }

    Client = sequin.NewClient(baseURL)
}
```

By default, the Client is initialized using Sequin's default host and port in local development: `http://localhost:7376`

## Usage

You'll predominantly use `sequin-go` to send, receive, and acknowledge [messages](https://github.com/sequinstream/sequin?tab=readme-ov-file#messages) in Sequin streams:

```go
package main

import (
    "fmt"
    "yourproject/sequin"
)

func main() {
    // Define your stream and consumer
    stream := "your-stream-name"
    consumer := "your-consumer-name"

    // Send a message
    res, err := sequin.Client.SendMessage(stream, "test.1", "Hello, Sequin!")
    if err != nil {
        fmt.Printf("Error sending message: %v\n", err)
        // Handle the error appropriately
    } else {
        fmt.Printf("Message sent successfully: %+v\n", res)
    }

    // Receive a message
    message, err := sequin.Client.ReceiveMessage(stream, consumer)
    if err != nil {
        fmt.Printf("Error receiving message: %v\n", err)
    } else if message == nil {
        fmt.Println("No messages available")
    } else {
        fmt.Printf("Received message: %+v\n", message)
        // Don't forget to acknowledge the message
        ackErr := sequin.AckMessage(stream, consumer, message.ackId)
        if ackErr != nil {
            fmt.Printf("Error acking message: %v\n", ackErr)
        } else {
            fmt.Println("Message acked")
        }
    }
}
```

## Testing

To adequately test Sequin, we recommend creating temporary streams and consumers in addition to testing sending and receiving messages. Here's an example using the `testing` package:

```go
package sequin_test

import (
    "testing"
    "time"
    "github.com/sequinstream/sequin-go"
)

func TestSequinStreamAndConsumer(t *testing.T) {
    client := sequin.NewClient("http://localhost:7673")

    streamName := "test-stream-" + time.Now().Format("20060102150405")
    consumerName := "test-consumer-" + time.Now().Format("20060102150405")

    // Create a new stream
    stream, err := client.CreateStream(streamName, nil)
    if err != nil {
        t.Fatalf("Error creating stream: %v", err)
    }
    if stream.Name != streamName {
        t.Fatalf("Expected stream name %s, got %s", streamName, stream.Name)
    }

    // Create a consumer
    consumer, err := client.CreateConsumer(streamName, consumerName, "test.>", nil)
    if err != nil {
        t.Fatalf("Error creating consumer: %v", err)
    }
    if consumer.Name != consumerName {
        t.Fatalf("Expected consumer name %s, got %s", consumerName, consumer.Name)
    }

    // Send a message
    res, err := client.SendMessage(streamName, "test.1", "Hello, Sequin!")
    if err != nil {
        t.Fatalf("Error sending message: %v", err)
    }
    if res.Published != 1 {
        t.Fatalf("Expected 1 message published, got %d", res.Published)
    }

    // Receive and ack a message
    message, err := client.ReceiveMessage(streamName, consumerName)
    if err != nil {
        t.Fatalf("Error receiving message: %v", err)
    }
    if message == nil {
        t.Fatal("Expected to receive a message, got nil")
    }

    err = client.AckMessage(streamName, consumerName, message.ackId)
    if err != nil {
        t.Fatalf("Error acking message: %v", err)
    }

    // Delete the consumer
    err = client.DeleteConsumer(streamName, consumerName)
    if err != nil {
        t.Fatalf("Error deleting consumer: %v", err)
    }

    // Delete the stream
    err = client.DeleteStream(streamName)
    if err != nil {
        t.Fatalf("Error deleting stream: %v", err)
    }
}
```

This test creates a temporary stream and consumer, sends a message, receives and acknowledges it, and then cleans up by deleting the consumer and stream. You can expand on this basic test to cover more of your specific use cases and edge cases.