// Sequin provides a  Go SDK for sending, receiving, and acknowledging messages in Sequin (https://github.com/sequinstream/sequin).

package sequin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type SequinClientInterface interface {
	CreateStream(name string, options *CreateStreamOptions) (*Stream, error)
	GetStream(id string) (*Stream, error)
	UpdateStream(id, name string) (*Stream, error)
	DeleteStream(id string) (*DeleteSuccess, error)
	SendMessage(streamIDOrName string, key string, data string) (*SendMessageResult, error)
	SendMessages(streamIDOrName string, messages []Message) (*SendMessageResult, error)
	ReceiveMessage(streamIDOrName string, consumerIDOrName string) (*ReceivedMessage, error)
	ReceiveMessages(streamIDOrName string, consumerIDOrName string, options *ReceiveMessagesOptions) ([]ReceivedMessage, error)
	AckMessage(streamIDOrName string, consumerIDOrName string, ackID string) (*AckSuccess, error)
	AckMessages(streamIDOrName string, consumerIDOrName string, ackIDs []string) (*AckSuccess, error)
	NackMessage(streamIDOrName string, consumerIDOrName string, ackID string) (*NackSuccess, error)
	NackMessages(streamIDOrName string, consumerIDOrName string, ackIDs []string) (*NackSuccess, error)
	CreateConsumer(streamIDOrName string, name string, filterKeyPattern string, options *CreateConsumerOptions) (*Consumer, error)
	GetConsumer(streamIDOrName string, consumerIDOrName string) (*Consumer, error)
	UpdateConsumer(streamIDOrName string, consumerIDOrName string, options *UpdateConsumerOptions) (*Consumer, error)
	DeleteConsumer(streamIDOrName string, consumerIDOrName string) (*DeleteSuccess, error)
	ListConsumers(streamIDOrName string) ([]Consumer, error)
	CreateWebhook(options *CreateWebhookOptions) (*Webhook, error)
	GetWebhook(webhookIDOrName string) (*Webhook, error)
	UpdateWebhook(webhookIDOrName string, options *UpdateWebhookOptions) (*Webhook, error)
	DeleteWebhook(webhookIDOrName string) (*DeleteSuccess, error)
	ListWebhooks() ([]Webhook, error)
	CreatePostgresDatabase(options *CreatePostgresDatabaseOptions) (*PostgresDatabase, error)
	GetPostgresDatabase(id string) (*PostgresDatabase, error)
	UpdatePostgresDatabase(id string, options *UpdatePostgresDatabaseOptions) (*PostgresDatabase, error)
	DeletePostgresDatabase(id string) (*DeleteSuccess, error)
	ListPostgresDatabases() ([]PostgresDatabase, error)
	TestPostgresDatabaseConnection(id string) (*TestConnectionResult, error)
	SetupPostgresDatabaseReplication(id string, options *SetupReplicationOptions) (*SetupReplicationResult, error)
	ListPostgresDatabaseSchemas(id string) ([]string, error)
	ListPostgresDatabaseTables(id string, schema string) ([]string, error)
	CreatePostgresReplication(options *CreatePostgresReplicationOptions) (*PostgresReplication, error)
	GetPostgresReplication(id string) (*PostgresReplication, error)
	UpdatePostgresReplication(id string, options *UpdatePostgresReplicationOptions) (*PostgresReplication, error)
	DeletePostgresReplication(id string) (*DeleteSuccess, error)
	ListPostgresReplications() ([]PostgresReplication, error)
	CreatePostgresReplicationBackfills(id string, tables []map[string]string) ([]string, error)
	CreateHttpEndpoint(options *CreateHttpEndpointOptions) (*HttpEndpoint, error)
	GetHttpEndpoint(id string) (*HttpEndpoint, error)
	UpdateHttpEndpoint(id string, options *UpdateHttpEndpointOptions) (*HttpEndpoint, error)
	DeleteHttpEndpoint(id string) (*DeleteSuccess, error)
	ListHttpEndpoints() ([]HttpEndpoint, error)
}

// Client represents a Sequin client for interacting with the Sequin API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

func NewClient(baseURL string, opts ...ClientOption) *Client {
	if baseURL == "" {
		baseURL = "http://localhost:7376"
	}
	c := &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type ClientOption func(*Client)

func WithAPIKey(apiKey string) ClientOption {
	return func(c *Client) {
		c.apiKey = apiKey
	}
}

// DeleteSuccess represents a successful deletion operation.
type DeleteSuccess struct {
	ID      string `json:"id"`      // ID of the deleted resource.
	Deleted bool   `json:"deleted"` // Indicates if the deletion was successful.
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

// request is an internal method for making HTTP requests to the Sequin API.
func (c *Client) request(endpoint, method string, body interface{}) ([]byte, error) {
	url := c.baseURL + endpoint
	var req *http.Request
	var err error

	if body != nil {
		var jsonBody []byte
		jsonBody, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		req, err = http.NewRequest(method, url, bytes.NewBuffer(jsonBody))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errorResponse struct {
			Summary          string                 `json:"summary"`
			ValidationErrors map[string]interface{} `json:"validation_errors"`
			Code             string                 `json:"code"`
		}
		if err := json.Unmarshal(responseBody, &errorResponse); err == nil {
			if errorResponse.ValidationErrors != nil {
				return nil, &ValidationError{
					Summary:          errorResponse.Summary,
					ValidationErrors: errorResponse.ValidationErrors,
					Code:             errorResponse.Code,
				}
			}
			return nil, fmt.Errorf("API error: %s", errorResponse.Summary)
		}
		return nil, fmt.Errorf("API error: status code %d", resp.StatusCode)
	}

	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(responseBody, &envelope); err == nil && envelope.Data != nil {
		return envelope.Data, nil
	}

	return responseBody, nil
}

// ValidationError represents an error that occurs during API validation
type ValidationError struct {
	Summary          string                 `json:"summary"`
	ValidationErrors map[string]interface{} `json:"validation_errors"`
	Code             string                 `json:"code"`
}

// Error implements the error interface for ValidationError
func (ve *ValidationError) Error() string {
	var parts []string
	if ve.Summary != "" {
		parts = append(parts, ve.Summary)
	}
	if len(ve.ValidationErrors) > 0 {
		for key, value := range ve.ValidationErrors {
			switch v := value.(type) {
			case []interface{}:
				for _, msg := range v {
					parts = append(parts, fmt.Sprintf("%s: %v", key, msg))
				}
			case string:
				parts = append(parts, fmt.Sprintf("%s: %s", key, v))
			default:
				parts = append(parts, fmt.Sprintf("%s: %v", key, v))
			}
		}
	}
	if len(parts) == 0 {
		return "An unknown validation error occurred"
	}
	return strings.Join(parts, "; ")
}

// IntPtr returns a pointer to the given int value.
func IntPtr(i int) *int {
	return &i
}

// StringPtr returns a pointer to the given string value.
func StringPtr(s string) *string {
	return &s
}

// BoolPtr returns a pointer to the given bool value.
func BoolPtr(b bool) *bool {
	return &b
}
