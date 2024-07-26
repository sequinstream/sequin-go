// Sequin provides a lightweight Go SDK for sending, receiving, and acknowledging messages in Sequin (https://github.com/sequinstream/sequin).

package sequin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Client represents a Sequin client for interacting with the Sequin API.
type Client struct {
	baseURL    string       // Base URL for the Sequin API.
	httpClient *http.Client // HTTP client for making API requests.
}

// DeleteSuccess represents a successful deletion operation.
type DeleteSuccess struct {
	ID      string `json:"id"`      // ID of the deleted resource.
	Deleted bool   `json:"deleted"` // Indicates if the deletion was successful.
}

// NewClient creates a new Sequin client with the given base URL.
// If no base URL is provided, it defaults to "http://localhost:7376".
func NewClient(baseURL string) *Client {
	if baseURL == "" {
		baseURL = "http://localhost:7376"
	}
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

// SendMessage [sends](https://github.com/sequinstream/sequin?tab=readme-ov-file#sending-messages) a single message to a stream.
func (c *Client) SendMessage(streamIDOrName string, key string, data string) (*SendMessageResult, error) {
	return c.SendMessages(streamIDOrName, []Message{{Key: key, Data: data}})
}

// SendMessages [sends](https://github.com/sequinstream/sequin?tab=readme-ov-file#sending-messages) a batch of messages to a stream. SendMessages is all or nothing. Either all the messages are successfully sent, or none of the messages are sent.
func (c *Client) SendMessages(streamIDOrName string, messages []Message) (*SendMessageResult, error) {
	body := map[string]interface{}{"messages": messages}
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/messages", streamIDOrName), "POST", body)
	if err != nil {
		return nil, err
	}

	var result SendMessageResult
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}

// ReceiveMessage receives a single message from a consumer.
func (c *Client) ReceiveMessage(streamIDOrName string, consumerIDOrName string) (*ReceivedMessage, error) {
	messages, err := c.ReceiveMessages(streamIDOrName, consumerIDOrName, &ReceiveMessagesOptions{BatchSize: 1})
	if err != nil {
		return nil, err
	}
	if len(messages) > 0 {
		return &messages[0], nil
	}
	return nil, nil
}

// ReceiveMessages receives a batch of messages from a consumer. Defaults to a batch of `10` messages:
func (c *Client) ReceiveMessages(streamIDOrName string, consumerIDOrName string, options *ReceiveMessagesOptions) ([]ReceivedMessage, error) {
	batchSize := 10
	if options != nil && options.BatchSize > 0 {
		batchSize = options.BatchSize
	}
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/consumers/%s/receive?batch_size=%d", streamIDOrName, consumerIDOrName, batchSize), "GET", nil)
	if err != nil {
		return nil, err
	}

	var messages []ReceivedMessage
	err = json.Unmarshal(responseBody, &messages)
	return messages, err
}

// AckMessage [acknowledge](https://github.com/sequinstream/sequin?tab=readme-ov-file#acking-messages) a single message.
func (c *Client) AckMessage(streamIDOrName string, consumerIDOrName string, ackID string) (*AckSuccess, error) {
	return c.AckMessages(streamIDOrName, consumerIDOrName, []string{ackID})
}

// AckMessages [acknowledge](https://github.com/sequinstream/sequin?tab=readme-ov-file#acking-messages) a batch of messages. AckMessages is all or nothing. Either all the messages are successfully acknowledged, or none of the messages are acknowledged.
func (c *Client) AckMessages(streamIDOrName string, consumerIDOrName string, ackIDs []string) (*AckSuccess, error) {
	body := map[string]interface{}{"ack_ids": ackIDs}
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/consumers/%s/ack", streamIDOrName, consumerIDOrName), "POST", body)
	if err != nil {
		return nil, err
	}

	var result AckSuccess
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}

// NackMessage [negatively acknowledges](https://github.com/sequinstream/sequin?tab=readme-ov-file#nacking-messages) a single message.
func (c *Client) NackMessage(streamIDOrName string, consumerIDOrName string, ackID string) (*NackSuccess, error) {
	return c.NackMessages(streamIDOrName, consumerIDOrName, []string{ackID})
}

// NackMessages [negatively acknowledges](https://github.com/sequinstream/sequin?tab=readme-ov-file#nacking-messages) a batch of messages. NackMessages is all or nothing. Either all the messages are successfully negatively acknowledged, or none of the messages are negatively acknowledged.
func (c *Client) NackMessages(streamIDOrName string, consumerIDOrName string, ackIDs []string) (*NackSuccess, error) {
	body := map[string]interface{}{"ack_ids": ackIDs}
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/consumers/%s/nack", streamIDOrName, consumerIDOrName), "POST", body)
	if err != nil {
		return nil, err
	}

	var result NackSuccess
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}

// CreateStream creates a new stream with the given name and options.
func (c *Client) CreateStream(streamName string, options ...*CreateStreamOptions) (*Stream, error) {
	body := map[string]interface{}{"name": streamName}

	if len(options) > 0 && options[0] != nil {
		opts := options[0]
		body["one_message_per_key"] = opts.OneMessagePerKey
		body["process_unmodified"] = opts.ProcessUnmodified
		body["max_storage_gb"] = opts.MaxStorageGB
		body["retain_up_to"] = opts.RetainUpTo
		body["retain_at_least"] = opts.RetainAtLeast
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

// CreateConsumer creates a new consumer for a stream.
func (c *Client) CreateConsumer(streamIDOrName string, consumerName string, consumerFilter string, options ...*CreateConsumerOptions) (*Consumer, error) {
	body := map[string]interface{}{
		"name":               consumerName,
		"filter_key_pattern": consumerFilter,
		"kind":               "pull",
	}
	if len(options) > 0 && options[0] != nil {
		opts := options[0]
		body["ack_wait_ms"] = opts.AckWaitMS
		body["max_ack_pending"] = opts.MaxAckPending
		body["max_deliver"] = opts.MaxDeliver
	}
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/consumers", streamIDOrName), "POST", body)
	if err != nil {
		return nil, err
	}

	var consumer Consumer
	err = json.Unmarshal(responseBody, &consumer)
	return &consumer, err
}

// DeleteConsumer deletes a consumer from a stream.
func (c *Client) DeleteConsumer(streamIDOrName string, consumerIDOrName string) (*DeleteSuccess, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/streams/%s/consumers/%s", streamIDOrName, consumerIDOrName), "DELETE", nil)
	if err != nil {
		return nil, err
	}

	var result DeleteSuccess
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}

// request is an internal method for making HTTP requests to the Sequin API.
func (c *Client) request(endpoint string, method string, body interface{}) ([]byte, error) {
	url := c.baseURL + endpoint
	var req *http.Request
	var err error

	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}

		req, err = http.NewRequest(method, url, bytes.NewBuffer(jsonBody))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}

	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		var errorResponse struct {
			Summary string `json:"summary"`
		}
		json.Unmarshal(responseBody, &errorResponse)
		return nil, fmt.Errorf("API error: %s", errorResponse.Summary)
	}

	// Check if the response is wrapped in a data envelope
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(responseBody, &envelope); err == nil && envelope.Data != nil {
		return envelope.Data, nil
	}

	return responseBody, nil
}
