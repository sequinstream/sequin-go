package sequin

import (
	"encoding/json"
	"fmt"
)

type PostgresDatabase struct {
	ID       string `json:"id"`
	Database string `json:"database"`
	Hostname string `json:"hostname"`
	PoolSize int    `json:"pool_size"`
	Port     int    `json:"port"`
	Name     string `json:"name"`
	SSL      bool   `json:"ssl"`
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

type CreatePostgresDatabaseOptions struct {
	Database string `json:"database"`
	Hostname string `json:"hostname"`
	PoolSize *int   `json:"pool_size,omitempty"`
	Port     int    `json:"port"`
	Name     string `json:"name"`
	SSL      *bool  `json:"ssl,omitempty"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type UpdatePostgresDatabaseOptions struct {
	Database *string `json:"database,omitempty"`
	Hostname *string `json:"hostname,omitempty"`
	PoolSize *int    `json:"pool_size,omitempty"`
	Port     *int    `json:"port,omitempty"`
	Name     *string `json:"name,omitempty"`
	SSL      *bool   `json:"ssl,omitempty"`
	Username *string `json:"username,omitempty"`
	Password *string `json:"password,omitempty"`
}

type TestConnectionResult struct {
	Success bool   `json:"success"`
	Reason  string `json:"reason,omitempty"`
}

type SetupReplicationOptions struct {
	SlotName        string     `json:"slot_name"`
	PublicationName string     `json:"publication_name"`
	Tables          [][]string `json:"tables"`
}

type SetupReplicationResult struct {
	Success         bool       `json:"success"`
	SlotName        string     `json:"slot_name"`
	PublicationName string     `json:"publication_name"`
	Tables          [][]string `json:"tables"`
}

func (c *Client) CreatePostgresDatabase(options *CreatePostgresDatabaseOptions) (*PostgresDatabase, error) {
	resp, err := c.request("/api/databases", "POST", options)
	if err != nil {
		return nil, err
	}

	var database PostgresDatabase
	err = json.Unmarshal(resp, &database)
	return &database, err
}

func (c *Client) GetPostgresDatabase(id string) (*PostgresDatabase, error) {
	resp, err := c.request(fmt.Sprintf("/api/databases/%s", id), "GET", nil)
	if err != nil {
		return nil, err
	}

	var database PostgresDatabase
	err = json.Unmarshal(resp, &database)
	return &database, err
}

func (c *Client) UpdatePostgresDatabase(id string, options *UpdatePostgresDatabaseOptions) (*PostgresDatabase, error) {
	resp, err := c.request(fmt.Sprintf("/api/databases/%s", id), "PUT", options)
	if err != nil {
		return nil, err
	}

	var database PostgresDatabase
	err = json.Unmarshal(resp, &database)
	return &database, err
}

func (c *Client) DeletePostgresDatabase(id string) (*DeleteSuccess, error) {
	resp, err := c.request(fmt.Sprintf("/api/databases/%s", id), "DELETE", nil)
	if err != nil {
		return nil, err
	}

	var result DeleteSuccess
	err = json.Unmarshal(resp, &result)
	return &result, err
}

func (c *Client) ListPostgresDatabases() ([]PostgresDatabase, error) {
	resp, err := c.request("/api/databases", "GET", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Data []PostgresDatabase `json:"data"`
	}
	err = json.Unmarshal(resp, &result)
	return result.Data, err
}

func (c *Client) TestPostgresDatabaseConnection(id string) (*TestConnectionResult, error) {
	resp, err := c.request(fmt.Sprintf("/api/databases/%s/test_connection", id), "POST", nil)
	if err != nil {
		return nil, err
	}

	var result TestConnectionResult
	err = json.Unmarshal(resp, &result)
	return &result, err
}

func (c *Client) SetupPostgresDatabaseReplication(id string, options *SetupReplicationOptions) (*SetupReplicationResult, error) {
	resp, err := c.request(fmt.Sprintf("/api/databases/%s/setup_replication", id), "POST", options)
	if err != nil {
		return nil, err
	}

	var result SetupReplicationResult
	err = json.Unmarshal(resp, &result)
	return &result, err
}

func (c *Client) ListPostgresDatabaseSchemas(id string) ([]string, error) {
	resp, err := c.request(fmt.Sprintf("/api/databases/%s/schemas", id), "GET", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Schemas []string `json:"schemas"`
	}
	err = json.Unmarshal(resp, &result)
	return result.Schemas, err
}

func (c *Client) ListPostgresDatabaseTables(id string, schema string) ([]string, error) {
	resp, err := c.request(fmt.Sprintf("/api/databases/%s/schemas/%s/tables", id, schema), "GET", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Tables []string `json:"tables"`
	}
	err = json.Unmarshal(resp, &result)
	return result.Tables, err
}
