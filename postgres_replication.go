package sequin

import (
	"encoding/json"
	"fmt"
	"time"
)

type PostgresReplication struct {
	ID                  string     `json:"id"`
	SlotName            string     `json:"slot_name"`
	PublicationName     string     `json:"publication_name"`
	Status              string     `json:"status"`
	AccountID           string     `json:"account_id"`
	PostgresDatabaseID  string     `json:"postgres_database_id"`
	SSL                 bool       `json:"ssl"`
	StreamID            string     `json:"stream_id"`
	CreatedAt           time.Time  `json:"inserted_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	BackfillCompletedAt *time.Time `json:"backfill_completed_at"`
	KeyFormat           string     `json:"key_format"`
}

type CreatePostgresReplicationOptions struct {
	SlotName           string `json:"slot_name"`
	PublicationName    string `json:"publication_name"`
	StreamID           string `json:"stream_id"`
	PostgresDatabaseID string `json:"postgres_database_id"`
	KeyFormat          string `json:"key_format,omitempty"`
	SSL                *bool  `json:"ssl,omitempty"`
}

type UpdatePostgresReplicationOptions struct {
	SlotName        *string `json:"slot_name,omitempty"`
	PublicationName *string `json:"publication_name,omitempty"`
	Status          *string `json:"status,omitempty"`
	KeyFormat       *string `json:"key_format,omitempty"`
	SSL             *bool   `json:"ssl,omitempty"`
}

func (c *Client) CreatePostgresReplication(options *CreatePostgresReplicationOptions) (*PostgresReplication, error) {
	responseBody, err := c.request("/api/postgres_replications", "POST", options)
	if err != nil {
		return nil, err
	}

	var result PostgresReplication
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}

func (c *Client) GetPostgresReplication(id string) (*PostgresReplication, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/postgres_replications/%s", id), "GET", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		PostgresReplication PostgresReplication `json:"postgres_replication"`
		Info                interface{}         `json:"info"`
	}
	err = json.Unmarshal(responseBody, &result)
	return &result.PostgresReplication, err
}

func (c *Client) UpdatePostgresReplication(id string, options *UpdatePostgresReplicationOptions) (*PostgresReplication, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/postgres_replications/%s", id), "PATCH", options)
	if err != nil {
		return nil, err
	}

	var result PostgresReplication
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}

func (c *Client) DeletePostgresReplication(id string) (*DeleteSuccess, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/postgres_replications/%s", id), "DELETE", nil)
	if err != nil {
		return nil, err
	}

	var result DeleteSuccess
	err = json.Unmarshal(responseBody, &result)
	return &result, err
}

func (c *Client) ListPostgresReplications() ([]PostgresReplication, error) {
	responseBody, err := c.request("/api/postgres_replications", "GET", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Data []PostgresReplication `json:"data"`
	}
	err = json.Unmarshal(responseBody, &result)
	return result.Data, err
}

func (c *Client) CreatePostgresReplicationBackfills(id string, tables []map[string]string) ([]string, error) {
	responseBody, err := c.request(fmt.Sprintf("/api/postgres_replications/%s/backfills", id), "POST", map[string]interface{}{
		"tables": tables,
	})
	if err != nil {
		return nil, err
	}

	var result struct {
		JobIDs []string `json:"job_ids"`
	}
	err = json.Unmarshal(responseBody, &result)
	return result.JobIDs, err
}
