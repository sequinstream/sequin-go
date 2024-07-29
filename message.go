package sequin

import "time"

// SendMessageEnvelope represents the envelope for sending a message to a Sequin stream.
type SendMessageEnvelope struct {
	Key  string `json:"key"`  // Key of the message.
	Data string `json:"data"` // Data payload of the message.
}

// ReceivedMessage represents a message received from a Sequin stream.
type ReceivedMessage struct {
	Message Message `json:"message"` // The received message.
	AckID   string  `json:"ack_id"`  // ID used to acknowledge the message.
}

// Message represents a detailed message in a Sequin stream.
type Message struct {
	Key        string    `json:"key"`         // Key of the message.
	StreamID   string    `json:"stream_id"`   // ID of the stream the message belongs to.
	Data       string    `json:"data"`        // Data payload of the message.
	Seq        int       `json:"seq"`         // Sequence number of the message in the stream.
	InsertedAt time.Time `json:"inserted_at"` // Timestamp when the message was inserted.
	UpdatedAt  time.Time `json:"updated_at"`  // Timestamp when the message was last updated.
}

// SendMessageResult represents the result of sending a message or batch of messages.
type SendMessageResult struct {
	Published int `json:"published"` // Number of messages successfully published.
}

// ReceiveMessagesOptions represents options for receiving messages.
type ReceiveMessagesOptions struct {
	BatchSize int `json:"batch_size"` // Number of messages to receive in a batch.
}
