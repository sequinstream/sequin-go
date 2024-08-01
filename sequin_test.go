package sequin

import (
	"fmt"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	client := NewClient("http://localhost:7376")
	testStreamName := fmt.Sprintf("test_stream_%s", time.Now().Format("20060102"))
	testConsumerName := fmt.Sprintf("test_consumer_%s", time.Now().Format("20060102"))

	t.Run("NewClient should create a new Client instance", func(t *testing.T) {
		if client == nil {
			t.Error("Expected client to be non-nil")
		}
	})

	t.Run("CreateStream should create a new stream", func(t *testing.T) {
		stream, err := client.CreateStream(testStreamName, nil)
		if err != nil {
			t.Fatalf("Error creating stream: %v", err)
		}
		if stream == nil {
			t.Fatal("Expected stream to be non-nil, but got nil")
		}
		if stream.ID == "" {
			t.Error("Expected stream to have an ID")
		}
		if stream.Name != testStreamName {
			t.Errorf("Expected stream name to be %s, got %s", testStreamName, stream.Name)
		}
	})

	t.Run("SendMessage should send a single message to the stream", func(t *testing.T) {
		result, err := client.SendMessage(testStreamName, "test.1", "value1")
		if err != nil {
			t.Errorf("Error sending message: %v", err)
		}
		if result.Published != 1 {
			t.Errorf("Expected 1 message published, got %d", result.Published)
		}
	})

	t.Run("SendMessages should send multiple messages to the stream", func(t *testing.T) {
		messages := []Message{
			{Key: "test.2", Data: "value2"},
			{Key: "test.3", Data: "value3"},
		}
		result, err := client.SendMessages(testStreamName, messages)
		if err != nil {
			t.Errorf("Error sending messages: %v", err)
		}
		if result.Published != 2 {
			t.Errorf("Expected 2 messages published, got %d", result.Published)
		}
	})

	t.Run("CreateConsumer should create a new consumer", func(t *testing.T) {
		consumer, err := client.CreateConsumer(testStreamName, testConsumerName, "test.>", nil)
		if err != nil {
			t.Errorf("Error creating consumer: %v", err)
		}
		if consumer.ID == "" {
			t.Error("Expected consumer to have an ID")
		}
		if consumer.Name != testConsumerName {
			t.Errorf("Expected consumer name to be %s, got %s", testConsumerName, consumer.Name)
		}
	})

	t.Run("ReceiveMessage should retrieve a single message", func(t *testing.T) {
		message, err := client.ReceiveMessage(testStreamName, testConsumerName)
		if err != nil {
			t.Errorf("Error receiving message: %v", err)
		}
		if message == nil {
			t.Error("Expected to receive a message, got nil")
		}
	})

	t.Run("ReceiveMessages should retrieve multiple messages", func(t *testing.T) {
		messages, err := client.ReceiveMessages(testStreamName, testConsumerName, &ReceiveMessagesOptions{BatchSize: 2})
		if err != nil {
			t.Errorf("Error receiving messages: %v", err)
		}
		if len(messages) != 2 {
			t.Errorf("Expected to receive 2 messages, got %d", len(messages))
		}
	})

	t.Run("ReceiveMessage should return null when no messages are available", func(t *testing.T) {
		// Ensure the stream is empty first
		_, err := client.ReceiveMessages(testStreamName, testConsumerName, &ReceiveMessagesOptions{BatchSize: 1000})
		if err != nil {
			t.Fatalf("Error receiving messages: %v", err)
		}

		message, err := client.ReceiveMessage(testStreamName, testConsumerName)
		if err != nil {
			t.Errorf("Error receiving message: %v", err)
		}
		if message != nil {
			t.Error("Expected to receive nil, got a message")
		}
	})

	t.Run("ReceiveMessages should respect custom batch_size", func(t *testing.T) {
		// Send 10 messages first
		messages := make([]Message, 10)
		for i := 0; i < 10; i++ {
			messages[i] = Message{Key: fmt.Sprintf("test.%d", i), Data: fmt.Sprintf("value_%d", i)}
		}
		_, err := client.SendMessages(testStreamName, messages)
		if err != nil {
			t.Fatalf("Error sending messages: %v", err)
		}

		receivedMessages, err := client.ReceiveMessages(testStreamName, testConsumerName, &ReceiveMessagesOptions{BatchSize: 3})
		if err != nil {
			t.Errorf("Error receiving messages: %v", err)
		}
		if len(receivedMessages) != 3 {
			t.Errorf("Expected to receive 3 messages, got %d", len(receivedMessages))
		}
	})

	t.Run("AckMessage should acknowledge a single message", func(t *testing.T) {
		// Send a message first
		_, err := client.SendMessage(testStreamName, "test.ack", "ack_value")
		if err != nil {
			t.Errorf("Error sending message: %v", err)
		}

		// Receive the message
		message, err := client.ReceiveMessage(testStreamName, testConsumerName)
		if err != nil {
			t.Errorf("Error receiving message: %v", err)
		}
		if message == nil {
			t.Fatal("Expected to receive a message, got nil")
		}

		// Acknowledge the message
		ackSuccess, err := client.AckMessage(testStreamName, testConsumerName, message.AckID)
		if err != nil {
			t.Errorf("Error acknowledging message: %v", err)
		}
		if !ackSuccess.Success {
			t.Error("Expected ack to be successful")
		}
	})

	t.Run("AckMessages should acknowledge multiple messages", func(t *testing.T) {
		receivedMessages, err := client.ReceiveMessages(testStreamName, testConsumerName, &ReceiveMessagesOptions{BatchSize: 2})
		if err != nil {
			t.Fatalf("Error receiving messages: %v", err)
		}

		ackIDs := make([]string, len(receivedMessages))
		for i, msg := range receivedMessages {
			ackIDs[i] = msg.AckID
		}

		ackSuccess, err := client.AckMessages(testStreamName, testConsumerName, ackIDs)
		if err != nil {
			t.Errorf("Error acknowledging messages: %v", err)
		}
		if !ackSuccess.Success {
			t.Error("Expected ack to be successful")
		}
	})

	t.Run("NackMessages should negative-acknowledge multiple messages", func(t *testing.T) {
		receivedMessages, err := client.ReceiveMessages(testStreamName, testConsumerName, &ReceiveMessagesOptions{BatchSize: 2})
		if err != nil {
			t.Fatalf("Error receiving messages: %v", err)
		}

		ackIDs := make([]string, len(receivedMessages))
		for i, msg := range receivedMessages {
			ackIDs[i] = msg.AckID
		}

		nackSuccess, err := client.NackMessages(testStreamName, testConsumerName, ackIDs)
		if err != nil {
			t.Errorf("Error negative-acknowledging messages: %v", err)
		}
		if !nackSuccess.Success {
			t.Error("Expected nack to be successful")
		}
	})

	t.Run("NackMessage should negative-acknowledge a single message", func(t *testing.T) {
		// Send a message first
		_, err := client.SendMessage(testStreamName, "test.nack", "nack_value")
		if err != nil {
			t.Errorf("Error sending message: %v", err)
		}

		// Receive the message
		message, err := client.ReceiveMessage(testStreamName, testConsumerName)
		if err != nil {
			t.Errorf("Error receiving message: %v", err)
		}
		if message == nil {
			t.Fatal("Expected to receive a message, got nil")
		}

		// Negative-acknowledge the message
		nackSuccess, err := client.NackMessage(testStreamName, testConsumerName, message.AckID)
		if err != nil {
			t.Errorf("Error nacking message: %v", err)
		}
		if !nackSuccess.Success {
			t.Error("Expected nack to be successful")
		}
	})

	t.Run("DeleteConsumer should delete a consumer", func(t *testing.T) {
		deleteSuccess, err := client.DeleteConsumer(testStreamName, testConsumerName)
		if err != nil {
			t.Errorf("Error deleting consumer: %v", err)
		}
		if !deleteSuccess.Deleted {
			t.Error("Expected consumer to be deleted")
		}
	})

	t.Run("DeleteStream should delete a stream", func(t *testing.T) {
		deleteSuccess, err := client.DeleteStream(testStreamName)
		if err != nil {
			t.Errorf("Error deleting stream: %v", err)
		}
		if !deleteSuccess.Deleted {
			t.Error("Expected stream to be deleted")
		}
	})

	// Additional tests can be added here to cover more scenarios
}
