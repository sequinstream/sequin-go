package sequin

import (
	"encoding/json"
	"fmt"
	"time"
)

// Webhook represents a Sequin webhook.
type Webhook struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	StreamID   string    `json:"stream_id"`
	InsertedAt time.Time `json:"inserted_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// CreateWebhookOptions represents options for creating a webhook.
type CreateWebhookOptions struct {
	Name     string `json:"name"`
	StreamID string `json:"stream_id"`
}

// UpdateWebhookOptions represents options for updating a webhook.
type UpdateWebhookOptions struct {
	Name *string `json:"name,omitempty"`
}

// CreateWebhook creates a new webhook.
func (c *Client) CreateWebhook(options *CreateWebhookOptions) (*Webhook, error) {
	responseBody, err := c.request("/api/webhooks", "POST", options)
	if err != nil {
		return nil, err
	}

	var webhook Webhook
	err = json.Unmarshal(responseBody, &webhook)
	return &webhook, err
}

// GetWebhook retrieves a webhook by its ID or name.
func (c *Client) GetWebhook(webhookIDOrName string) (*Webhook, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/webhooks/%s", webhookIDOrName), "GET", nil)
	if err != nil {
		return nil, err
	}

	var webhook Webhook
	err = json.Unmarshal(responseBody, &webhook)
	return &webhook, err
}

// UpdateWebhook updates a webhook by its ID or name.
func (c *Client) UpdateWebhook(webhookIDOrName string, options *UpdateWebhookOptions) (*Webhook, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/webhooks/%s", webhookIDOrName), "PUT", options)
	if err != nil {
		return nil, err
	}

	var webhook Webhook
	err = json.Unmarshal(responseBody, &webhook)
	return &webhook, err
}

// DeleteWebhook deletes a webhook by its ID or name.
func (c *Client) DeleteWebhook(webhookIDOrName string) (*DeleteSuccess, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/webhooks/%s", webhookIDOrName), "DELETE", nil)
	if err != nil {
		return nil, err
	}

	var result DeleteSuccess
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}

// ListWebhooks retrieves all webhooks.
func (c *Client) ListWebhooks() ([]Webhook, error) {
	responseBody, err := c.request("/api/webhooks", "GET", nil)
	if err != nil {
		return nil, err
	}

	var response struct {
		Data []Webhook `json:"data"`
	}
	err = json.Unmarshal(responseBody, &response)
	return response.Data, err
}
