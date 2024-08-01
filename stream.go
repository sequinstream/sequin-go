package sequin

import (
	"encoding/json"
	"fmt"
	"time"
)

// CreateStreamOptions represents options for creating a stream.
type CreateStreamOptions struct {
	OneMessagePerKey  bool `json:"one_message_per_key,omitempty"`
	ProcessUnmodified bool `json:"process_unmodified,omitempty"`
	MaxStorageGB      int  `json:"max_storage_gb,omitempty"`
	RetainUpTo        int  `json:"retain_up_to,omitempty"`
	RetainAtLeast     int  `json:"retain_at_least,omitempty"`
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

// GetStream retrieves a stream by its ID or name.
func (c *Client) GetStream(streamIDOrName string) (*Stream, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s", streamIDOrName), "GET", nil)
	if err != nil {
		return nil, err
	}

	var stream Stream
	err = json.Unmarshal(responseBody, &stream)
	return &stream, err
}

// UpdateStream updates a stream by its ID or name.
func (c *Client) UpdateStream(streamIDOrName string, name string) (*Stream, error) {
	body := map[string]interface{}{"name": name}
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s", streamIDOrName), "PUT", body)
	if err != nil {
		return nil, err
	}

	var stream Stream
	err = json.Unmarshal(responseBody, &stream)
	return &stream, err
}

// CreateStream creates a new stream with the given name and options.
func (c *Client) CreateStream(streamName string, options *CreateStreamOptions) (*Stream, error) {
	body := map[string]interface{}{"name": streamName}

	if options != nil {
		body["one_message_per_key"] = options.OneMessagePerKey
		body["process_unmodified"] = options.ProcessUnmodified
		body["max_storage_gb"] = options.MaxStorageGB
		body["retain_up_to"] = options.RetainUpTo
		body["retain_at_least"] = options.RetainAtLeast
	}

	responseBody, err := c.request("/api/streams", "POST", body)
	if err != nil {
		return nil, err
	}

	var stream Stream
	err = json.Unmarshal(responseBody, &stream)
	return &stream, err
}

// DeleteStream deletes a stream by its ID or name.
func (c *Client) DeleteStream(streamIDOrName string) (*DeleteSuccess, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s", streamIDOrName), "DELETE", nil)
	if err != nil {
		return nil, err
	}

	var result DeleteSuccess
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}
