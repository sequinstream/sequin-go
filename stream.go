package sequin

import "time"

// CreateStreamOptions represents options for creating a stream.
type CreateStreamOptions struct {
	OneMessagePerKey  bool `json:"one_message_per_key,omitempty"` // If true, only one message per key is allowed in the stream. Default is false.
	ProcessUnmodified bool `json:"process_unmodified,omitempty"`  // If true, unmodified messages will be processed. Default is false.
	MaxStorageGB      int  `json:"max_storage_gb,omitempty"`      // Maximum storage size for the stream in gigabytes.
	RetainUpTo        int  `json:"retain_up_to,omitempty"`        // Maximum number of messages to retain in the stream.
	RetainAtLeast     int  `json:"retain_at_least,omitempty"`     // Minimum number of messages to retain in the stream.
}

// Stream represents a Sequin stream.
type Stream struct {
	ID         string    `json:"id"`          // Unique identifier for the stream.
	Name       string    `json:"name"`        // Name of the stream.
	AccountID  string    `json:"account_id"`  // ID of the account that owns the stream.
	Stats      Stats     `json:"stats"`       // Statistics for the stream.
	InsertedAt time.Time `json:"inserted_at"` // Timestamp when the stream was created.
	UpdatedAt  time.Time `json:"updated_at"`  // Timestamp when the stream was last updated.
}

// Stats represents stream statistics.
type Stats struct {
	MessageCount  int `json:"message_count"`  // Number of messages in the stream.
	ConsumerCount int `json:"consumer_count"` // Number of consumers for the stream.
	StorageSize   int `json:"storage_size"`   // Current storage size of the stream in bytes.
}
