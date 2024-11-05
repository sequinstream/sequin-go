package sequin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// SequinClient defines the interface for Sequin client operations
type SequinClient interface {
	Receive(ctx context.Context, consumerGroupID string, params *ReceiveParams) ([]Message, error)
	Ack(ctx context.Context, consumerGroupID string, ackIDs []string) error
	Nack(ctx context.Context, consumerGroupID string, ackIDs []string) error
}

// Client represents a Sequin client
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// Ensure Client implements SequinClient interface
var _ SequinClient = (*Client)(nil)

// ClientOptions configures the client behavior
type ClientOptions struct {
	BaseURL    string        // API base URL, defaults to "https://api.sequinstream.com/api"
	HTTPClient *http.Client  // Custom HTTP client, optional
	Timeout    time.Duration // HTTP client timeout, defaults to 30s
}

// NewClient creates a new Sequin client
func NewClient(token string, opts *ClientOptions) *Client {
	if opts == nil {
		opts = &ClientOptions{}
	}

	if opts.BaseURL == "" {
		opts.BaseURL = "https://api.sequinstream.com/api"
	}

	if opts.HTTPClient == nil {
		timeout := opts.Timeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		opts.HTTPClient = &http.Client{
			Timeout: timeout,
		}
	}

	return &Client{
		baseURL:    opts.BaseURL,
		token:      token,
		httpClient: opts.HTTPClient,
	}
}

// ReceiveResponse represents the response from the receive endpoint
type ReceiveResponse struct {
	Data []struct {
		AckID string `json:"ack_id"`
		Data  struct {
			Record json.RawMessage `json:"record"`
		} `json:"data"`
	} `json:"data"`
}

// ReceiveParams represents parameters for the receive request
type ReceiveParams struct {
	BatchSize int `json:"batch_size,omitempty"`
	WaitFor   int `json:"wait_for,omitempty"` // milliseconds
}

// Receive fetches messages from a consumer
func (c *Client) Receive(ctx context.Context, consumerGroupID string, params *ReceiveParams) ([]Message, error) {
	url := fmt.Sprintf("%s/api/http_pull_consumers/%s/receive", c.baseURL, consumerGroupID)

	var body []byte
	var err error
	if params != nil {
		body, err = json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("marshaling receive params: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var receiveResp ReceiveResponse
	if err := json.NewDecoder(resp.Body).Decode(&receiveResp); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	messages := make([]Message, len(receiveResp.Data))
	for i, msg := range receiveResp.Data {
		messages[i] = Message{
			AckID:  msg.AckID,
			Record: msg.Data.Record,
		}
	}

	return messages, nil
}

// Ack acknowledges messages as processed
func (c *Client) Ack(ctx context.Context, consumerGroupID string, ackIDs []string) error {
	url := fmt.Sprintf("%s/api/http_pull_consumers/%s/ack", c.baseURL, consumerGroupID)

	body, err := json.Marshal(map[string][]string{
		"ack_ids": ackIDs,
	})
	if err != nil {
		return fmt.Errorf("marshaling ack request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// Nack negative acknowledges messages, making them available for redelivery
func (c *Client) Nack(ctx context.Context, consumerGroupID string, ackIDs []string) error {
	url := fmt.Sprintf("%s/api/http_pull_consumers/%s/nack", c.baseURL, consumerGroupID)

	body, err := json.Marshal(map[string][]string{
		"ack_ids": ackIDs,
	})
	if err != nil {
		return fmt.Errorf("marshaling nack request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %w", err)
	}

	return nil
}

// Message represents a single message with its acknowledgment ID
type Message struct {
	AckID  string
	Record json.RawMessage
}
