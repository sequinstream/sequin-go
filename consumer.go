package sequin

import (
	"encoding/json"
	"fmt"
	"time"
)

// CreateConsumerOptions represents options for creating a consumer.
type CreateConsumerOptions struct {
	AckWaitMS      int    `json:"ack_wait_ms,omitempty"`     // Acknowledgement wait time in milliseconds.
	MaxAckPending  int    `json:"max_ack_pending,omitempty"` // Maximum number of pending acknowledgements.
	MaxDeliver     int    `json:"max_deliver,omitempty"`     // Maximum number of delivery attempts.
	Kind           string `json:"kind,omitempty"`            // Kind of consumer (e.g., "pull" or "push").
	HttpEndpointId string `json:"http_endpoint_id,omitempty"`
}

// UpdateConsumerOptions represents options for updating a consumer.
type UpdateConsumerOptions struct {
	AckWaitMS     *int    `json:"ack_wait_ms,omitempty"`     // Acknowledgement wait time in milliseconds.
	MaxAckPending *int    `json:"max_ack_pending,omitempty"` // Maximum number of pending acknowledgements.
	MaxDeliver    *int    `json:"max_deliver,omitempty"`     // Maximum number of delivery attempts.
	Kind          *string `json:"kind,omitempty"`            // Kind of consumer (e.g., "pull" or "push").
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
	HttpEndpointId   string    `json:"http_endpoint_id,omitempty"`
}

// AckSuccess represents a successful acknowledgement operation.
type AckSuccess struct {
	Success bool `json:"success"` // Indicates if the acknowledgement was successful.
}

// NackSuccess represents a successful negative acknowledgement operation.
type NackSuccess struct {
	Success bool `json:"success"` // Indicates if the negative acknowledgement was successful.
}

// CreateConsumer creates a new consumer for a stream.
func (c *Client) CreateConsumer(streamIDOrName string, name string, filterKeyPattern string, options *CreateConsumerOptions) (*Consumer, error) {
	body := map[string]interface{}{
		"name":               name,
		"filter_key_pattern": filterKeyPattern,
		"kind":               "pull",
	}

	if options != nil {
		body["ack_wait_ms"] = options.AckWaitMS
		body["max_ack_pending"] = options.MaxAckPending
		body["max_deliver"] = options.MaxDeliver
		body["kind"] = options.Kind
		if options.HttpEndpointId != "" {
			body["http_endpoint_id"] = options.HttpEndpointId
		}
	}

	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/consumers", streamIDOrName), "POST", body)
	if err != nil {
		return nil, err
	}

	var consumer Consumer
	err = json.Unmarshal(responseBody, &consumer)
	return &consumer, err
}

// GetConsumer retrieves a consumer by its ID or name.
func (c *Client) GetConsumer(streamIDOrName string, consumerIDOrName string) (*Consumer, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/consumers/%s", streamIDOrName, consumerIDOrName), "GET", nil)
	if err != nil {
		return nil, err
	}

	var consumer Consumer
	err = json.Unmarshal(responseBody, &consumer)
	return &consumer, err
}

// UpdateConsumer updates a consumer by its ID or name.
func (c *Client) UpdateConsumer(streamIDOrName string, consumerIDOrName string, options *UpdateConsumerOptions) (*Consumer, error) {
	body := map[string]interface{}{}

	if options != nil {
		if options.AckWaitMS != nil {
			body["ack_wait_ms"] = *options.AckWaitMS
		}
		if options.MaxAckPending != nil {
			body["max_ack_pending"] = *options.MaxAckPending
		}
		if options.MaxDeliver != nil {
			body["max_deliver"] = *options.MaxDeliver
		}
		if options.Kind != nil {
			body["kind"] = *options.Kind
		}
	}

	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/consumers/%s", streamIDOrName, consumerIDOrName), "PUT", body)
	if err != nil {
		return nil, err
	}

	var consumer Consumer
	err = json.Unmarshal(responseBody, &consumer)
	return &consumer, err
}

// DeleteConsumer deletes a consumer by its ID or name.
func (c *Client) DeleteConsumer(streamIDOrName string, consumerIDOrName string) (*DeleteSuccess, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/consumers/%s", streamIDOrName, consumerIDOrName), "DELETE", nil)
	if err != nil {
		return nil, err
	}

	var result DeleteSuccess
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}

// ListConsumers retrieves all consumers for a stream.
func (c *Client) ListConsumers(streamIDOrName string) ([]Consumer, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/consumers", streamIDOrName), "GET", nil)
	if err != nil {
		return nil, err
	}

	var response struct {
		Data []Consumer `json:"data"`
	}
	err = json.Unmarshal(responseBody, &response)
	return response.Data, err
}
