package sequin

import (
	"encoding/json"
	"fmt"
	"time"
)

// HttpEndpoint represents a Sequin HTTP endpoint.
type HttpEndpoint struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	BaseURL    string            `json:"base_url"`
	Headers    map[string]string `json:"headers"`
	AccountID  string            `json:"account_id"`
	InsertedAt time.Time         `json:"inserted_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// CreateHttpEndpointOptions represents options for creating an HTTP endpoint.
type CreateHttpEndpointOptions struct {
	Name    string            `json:"name"`
	BaseURL string            `json:"base_url"`
	Headers map[string]string `json:"headers,omitempty"`
}

// UpdateHttpEndpointOptions represents options for updating an HTTP endpoint.
type UpdateHttpEndpointOptions struct {
	Name    *string            `json:"name,omitempty"`
	BaseURL *string            `json:"base_url,omitempty"`
	Headers *map[string]string `json:"headers,omitempty"`
}

// CreateHttpEndpoint creates a new HTTP endpoint.
func (c *Client) CreateHttpEndpoint(options *CreateHttpEndpointOptions) (*HttpEndpoint, error) {
	responseBody, err := c.request("/api/http_endpoints", "POST", options)
	if err != nil {
		return nil, err
	}

	var httpEndpoint HttpEndpoint
	err = json.Unmarshal(responseBody, &httpEndpoint)
	return &httpEndpoint, err
}

// GetHttpEndpoint retrieves an HTTP endpoint by its ID.
func (c *Client) GetHttpEndpoint(id string) (*HttpEndpoint, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/http_endpoints/%s", id), "GET", nil)
	if err != nil {
		return nil, err
	}

	var httpEndpoint HttpEndpoint
	err = json.Unmarshal(responseBody, &httpEndpoint)
	return &httpEndpoint, err
}

// UpdateHttpEndpoint updates an HTTP endpoint by its ID.
func (c *Client) UpdateHttpEndpoint(id string, options *UpdateHttpEndpointOptions) (*HttpEndpoint, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/http_endpoints/%s", id), "PUT", options)
	if err != nil {
		return nil, err
	}

	var httpEndpoint HttpEndpoint
	err = json.Unmarshal(responseBody, &httpEndpoint)
	return &httpEndpoint, err
}

// DeleteHttpEndpoint deletes an HTTP endpoint by its ID.
func (c *Client) DeleteHttpEndpoint(id string) (*DeleteSuccess, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/http_endpoints/%s", id), "DELETE", nil)
	if err != nil {
		return nil, err
	}

	var result DeleteSuccess
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}

// ListHttpEndpoints retrieves all HTTP endpoints for the account.
func (c *Client) ListHttpEndpoints() ([]HttpEndpoint, error) {
	responseBody, err := c.request("/api/http_endpoints", "GET", nil)
	if err != nil {
		return nil, err
	}

	var response struct {
		Data []HttpEndpoint `json:"data"`
	}
	err = json.Unmarshal(responseBody, &response)
	return response.Data, err
}
