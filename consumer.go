package sequin

import "time"

// CreateConsumerOptions represents options for creating a consumer.
type CreateConsumerOptions struct {
	AckWaitMS     int `json:"ack_wait_ms,omitempty"`     // Acknowledgement wait time in milliseconds.
	MaxAckPending int `json:"max_ack_pending,omitempty"` // Maximum number of pending acknowledgements.
	MaxDeliver    int `json:"max_deliver,omitempty"`     // Maximum number of delivery attempts.
}

// Consumer represents a Sequin consumer.
type Consumer struct {
	ID               string    `json:"id"`                 // Unique identifier for the consumer.
	Name             string    `json:"name"`               // Name of the consumer.
	StreamID         string    `json:"stream_id"`          // ID of the stream the consumer is attached to.
	FilterKeyPattern string    `json:"filter_key_pattern"` // Pattern used to filter messages for this consumer.
	AckWaitMS        int       `json:"ack_wait_ms"`        // Acknowledgement wait time in milliseconds.
	MaxAckPending    int       `json:"max_ack_pending"`    // Maximum number of pending acknowledgements.
	MaxDeliver       int       `json:"max_deliver"`        // Maximum number of delivery attempts.
	MaxWaiting       int       `json:"max_waiting"`        // Maximum number of waiting messages.
	Kind             string    `json:"kind"`               // Kind of consumer (e.g., "pull").
	InsertedAt       time.Time `json:"inserted_at"`        // Timestamp when the consumer was created.
	UpdatedAt        time.Time `json:"updated_at"`         // Timestamp when the consumer was last updated.
	Status           string    `json:"status"`             // Current status of the consumer.
}

// AckSuccess represents a successful acknowledgement operation.
type AckSuccess struct {
	Success bool `json:"success"` // Indicates if the acknowledgement was successful.
}

// NackSuccess represents a successful negative acknowledgement operation.
type NackSuccess struct {
	Success bool `json:"success"` // Indicates if the negative acknowledgement was successful.
}
