package sequin

import (
	"fmt"
	"sync"
	"time"
)

type MockClient struct {
	streams              map[string]*Stream
	webhooks             map[string]*Webhook
	postgresDatabases    map[string]*PostgresDatabase
	postgresReplications map[string]*PostgresReplication
	httpEndpoints        map[string]*HttpEndpoint
	consumers            map[string]*Consumer
	mu                   sync.Mutex
}

func NewMockClient() *MockClient {
	return &MockClient{
		streams:              make(map[string]*Stream),
		webhooks:             make(map[string]*Webhook),
		postgresDatabases:    make(map[string]*PostgresDatabase),
		postgresReplications: make(map[string]*PostgresReplication),
		httpEndpoints:        make(map[string]*HttpEndpoint),
		consumers:            make(map[string]*Consumer),
	}
}

func (m *MockClient) CreateStream(name string, options *CreateStreamOptions) (*Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := fmt.Sprintf("stream-%d", len(m.streams)+1)
	stream := &Stream{
		ID:   id,
		Name: name,
	}
	m.streams[id] = stream
	return stream, nil
}

func (m *MockClient) GetStream(id string) (*Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.streams[id]
	if !ok {
		return nil, fmt.Errorf("stream not found")
	}
	return stream, nil
}

func (m *MockClient) UpdateStream(id, name string) (*Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.streams[id]
	if !ok {
		return nil, fmt.Errorf("stream not found")
	}
	stream.Name = name
	return stream, nil
}

func (m *MockClient) DeleteStream(id string) (*DeleteSuccess, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.streams[id]; !ok {
		return nil, fmt.Errorf("stream not found")
	}
	delete(m.streams, id)
	return &DeleteSuccess{ID: id, Deleted: true}, nil
}

func (m *MockClient) AckMessage(streamIDOrName string, consumerIDOrName string, ackID string) (*AckSuccess, error) {
	// Implement mock behavior here
	return &AckSuccess{Success: true}, nil
}

func (m *MockClient) SendMessage(streamIDOrName string, key string, data string) (*SendMessageResult, error) {
	// Implement mock behavior here
	return &SendMessageResult{Published: 1}, nil
}

func (m *MockClient) SendMessages(streamIDOrName string, messages []Message) (*SendMessageResult, error) {
	// Implement mock behavior here
	return &SendMessageResult{Published: len(messages)}, nil
}

func (m *MockClient) ReceiveMessage(streamIDOrName string, consumerIDOrName string) (*ReceivedMessage, error) {
	// Implement mock behavior here
	return &ReceivedMessage{}, nil
}

func (m *MockClient) ReceiveMessages(streamIDOrName string, consumerIDOrName string, options *ReceiveMessagesOptions) ([]ReceivedMessage, error) {
	// Implement mock behavior here
	return []ReceivedMessage{}, nil
}

func (m *MockClient) AckMessages(streamIDOrName string, consumerIDOrName string, ackIDs []string) (*AckSuccess, error) {
	// Implement mock behavior here
	return &AckSuccess{Success: true}, nil
}

func (m *MockClient) NackMessage(streamIDOrName string, consumerIDOrName string, ackID string) (*NackSuccess, error) {
	// Implement mock behavior here
	return &NackSuccess{Success: true}, nil
}

func (m *MockClient) NackMessages(streamIDOrName string, consumerIDOrName string, ackIDs []string) (*NackSuccess, error) {
	// Implement mock behavior here
	return &NackSuccess{Success: true}, nil
}

func (m *MockClient) CreateConsumer(streamIDOrName string, name string, filterKeyPattern string, options *CreateConsumerOptions) (*Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := fmt.Sprintf("consumer-%d", len(m.consumers)+1)
	consumer := &Consumer{
		ID:               id,
		StreamID:         streamIDOrName,
		Name:             name,
		FilterKeyPattern: filterKeyPattern,
		AckWaitMS:        options.AckWaitMS,
		MaxAckPending:    options.MaxAckPending,
		MaxDeliver:       options.MaxDeliver,
		Kind:             options.Kind,
		HttpEndpointId:   options.HttpEndpointId,
	}
	m.consumers[id] = consumer
	return consumer, nil
}

func (m *MockClient) GetConsumer(streamIDOrName string, consumerIDOrName string) (*Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	consumer, ok := m.consumers[consumerIDOrName]
	if !ok {
		return nil, fmt.Errorf("consumer not found")
	}
	return consumer, nil
}

func (m *MockClient) UpdateConsumer(streamIDOrName string, consumerIDOrName string, options *UpdateConsumerOptions) (*Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	consumer, ok := m.consumers[consumerIDOrName]
	if !ok {
		return nil, fmt.Errorf("consumer not found")
	}

	if options.AckWaitMS != nil {
		consumer.AckWaitMS = *options.AckWaitMS
	}
	if options.MaxAckPending != nil {
		consumer.MaxAckPending = *options.MaxAckPending
	}
	if options.MaxDeliver != nil {
		consumer.MaxDeliver = *options.MaxDeliver
	}
	if options.Kind != nil {
		consumer.Kind = *options.Kind
	}
	// Note: We don't update HttpEndpointId here to match the backend API behavior

	return consumer, nil
}

func (m *MockClient) DeleteConsumer(streamIDOrName string, consumerIDOrName string) (*DeleteSuccess, error) {
	// Implement mock behavior here
	return &DeleteSuccess{Deleted: true}, nil
}

func (m *MockClient) ListConsumers(streamIDOrName string) ([]Consumer, error) {
	// Implement mock behavior here
	return []Consumer{}, nil
}

func (m *MockClient) CreateWebhook(options *CreateWebhookOptions) (*Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := fmt.Sprintf("webhook-%d", len(m.webhooks)+1)
	webhook := &Webhook{
		ID:       id,
		Name:     options.Name,
		StreamID: options.StreamID,
	}
	m.webhooks[id] = webhook
	return webhook, nil
}

func (m *MockClient) GetWebhook(webhookIDOrName string) (*Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	webhook, ok := m.webhooks[webhookIDOrName]
	if !ok {
		return nil, fmt.Errorf("webhook not found")
	}
	return webhook, nil
}

func (m *MockClient) UpdateWebhook(webhookIDOrName string, options *UpdateWebhookOptions) (*Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	webhook, ok := m.webhooks[webhookIDOrName]
	if !ok {
		return nil, fmt.Errorf("webhook not found")
	}
	if options.Name != nil {
		webhook.Name = *options.Name
	}
	return webhook, nil
}

func (m *MockClient) DeleteWebhook(webhookIDOrName string) (*DeleteSuccess, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.webhooks[webhookIDOrName]; !ok {
		return nil, fmt.Errorf("webhook not found")
	}
	delete(m.webhooks, webhookIDOrName)
	return &DeleteSuccess{ID: webhookIDOrName, Deleted: true}, nil
}

func (m *MockClient) ListWebhooks() ([]Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	webhooks := make([]Webhook, 0, len(m.webhooks))
	for _, webhook := range m.webhooks {
		webhooks = append(webhooks, *webhook)
	}
	return webhooks, nil
}

func (m *MockClient) CreatePostgresDatabase(options *CreatePostgresDatabaseOptions) (*PostgresDatabase, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := fmt.Sprintf("postgres-database-%d", len(m.postgresDatabases)+1)
	database := &PostgresDatabase{
		ID:       id,
		Database: options.Database,
		Hostname: options.Hostname,
		Port:     options.Port,
		Name:     options.Name,
		Username: options.Username,
		Password: options.Password,
		PoolSize: *options.PoolSize,
		SSL:      *options.SSL,
	}
	m.postgresDatabases[id] = database
	return database, nil
}

func (m *MockClient) GetPostgresDatabase(id string) (*PostgresDatabase, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	database, ok := m.postgresDatabases[id]
	if !ok {
		return nil, fmt.Errorf("postgres database not found")
	}
	return database, nil
}

func (m *MockClient) UpdatePostgresDatabase(id string, options *UpdatePostgresDatabaseOptions) (*PostgresDatabase, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	database, ok := m.postgresDatabases[id]
	if !ok {
		return nil, fmt.Errorf("postgres database not found")
	}

	if options.Database != nil {
		database.Database = *options.Database
	}
	if options.Hostname != nil {
		database.Hostname = *options.Hostname
	}
	if options.Port != nil {
		database.Port = *options.Port
	}
	if options.Name != nil {
		database.Name = *options.Name
	}
	if options.Username != nil {
		database.Username = *options.Username
	}
	if options.Password != nil {
		database.Password = *options.Password
	}
	if options.PoolSize != nil {
		database.PoolSize = *options.PoolSize
	}
	if options.SSL != nil {
		database.SSL = *options.SSL
	}

	return database, nil
}

func (m *MockClient) DeletePostgresDatabase(id string) (*DeleteSuccess, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.postgresDatabases[id]; !ok {
		return nil, fmt.Errorf("postgres database not found")
	}
	delete(m.postgresDatabases, id)
	return &DeleteSuccess{ID: id, Deleted: true}, nil
}

func (m *MockClient) ListPostgresDatabases() ([]PostgresDatabase, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	databases := make([]PostgresDatabase, 0, len(m.postgresDatabases))
	for _, database := range m.postgresDatabases {
		databases = append(databases, *database)
	}
	return databases, nil
}

func (m *MockClient) TestPostgresDatabaseConnection(id string) (*TestConnectionResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.postgresDatabases[id]; !ok {
		return &TestConnectionResult{Success: false, Reason: "postgres database not found"}, nil
	}
	return &TestConnectionResult{Success: true}, nil
}

func (m *MockClient) SetupPostgresDatabaseReplication(id string, options *SetupReplicationOptions) (*SetupReplicationResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.postgresDatabases[id]; !ok {
		return nil, fmt.Errorf("postgres database not found")
	}
	return &SetupReplicationResult{
		Success:         true,
		SlotName:        options.SlotName,
		PublicationName: options.PublicationName,
		Tables:          options.Tables,
	}, nil
}

func (m *MockClient) ListPostgresDatabaseSchemas(id string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.postgresDatabases[id]; !ok {
		return nil, fmt.Errorf("postgres database not found")
	}
	return []string{"public", "schema1", "schema2"}, nil
}

func (m *MockClient) ListPostgresDatabaseTables(id string, schema string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.postgresDatabases[id]; !ok {
		return nil, fmt.Errorf("postgres database not found")
	}
	return []string{"table1", "table2", "table3"}, nil
}

func (m *MockClient) CreatePostgresReplication(options *CreatePostgresReplicationOptions) (*PostgresReplication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := fmt.Sprintf("replication-%d", len(m.postgresReplications)+1)
	replication := &PostgresReplication{
		ID:                 id,
		SlotName:           options.SlotName,
		PublicationName:    options.PublicationName,
		StreamID:           options.StreamID,
		PostgresDatabaseID: options.PostgresDatabaseID,
		KeyFormat:          options.KeyFormat,
		SSL:                *options.SSL,
		Status:             "active",
	}
	m.postgresReplications[id] = replication
	return replication, nil
}

func (m *MockClient) GetPostgresReplication(id string) (*PostgresReplication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	replication, ok := m.postgresReplications[id]
	if !ok {
		return nil, fmt.Errorf("postgres replication not found")
	}
	return replication, nil
}

func (m *MockClient) UpdatePostgresReplication(id string, options *UpdatePostgresReplicationOptions) (*PostgresReplication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	replication, ok := m.postgresReplications[id]
	if !ok {
		return nil, fmt.Errorf("postgres replication not found")
	}

	if options.SlotName != nil {
		replication.SlotName = *options.SlotName
	}
	if options.PublicationName != nil {
		replication.PublicationName = *options.PublicationName
	}
	if options.Status != nil {
		replication.Status = *options.Status
	}
	if options.KeyFormat != nil {
		replication.KeyFormat = *options.KeyFormat
	}
	if options.SSL != nil {
		replication.SSL = *options.SSL
	}

	return replication, nil
}

func (m *MockClient) DeletePostgresReplication(id string) (*DeleteSuccess, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.postgresReplications[id]; !ok {
		return nil, fmt.Errorf("postgres replication not found")
	}
	delete(m.postgresReplications, id)
	return &DeleteSuccess{ID: id, Deleted: true}, nil
}

func (m *MockClient) ListPostgresReplications() ([]PostgresReplication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	replications := make([]PostgresReplication, 0, len(m.postgresReplications))
	for _, replication := range m.postgresReplications {
		replications = append(replications, *replication)
	}
	return replications, nil
}

func (m *MockClient) CreatePostgresReplicationBackfills(id string, tables []map[string]string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.postgresReplications[id]; !ok {
		return nil, fmt.Errorf("postgres replication not found")
	}

	jobIDs := make([]string, len(tables))
	for i := range tables {
		jobIDs[i] = fmt.Sprintf("job-%d", i+1)
	}
	return jobIDs, nil
}

func (m *MockClient) CreateHttpEndpoint(options *CreateHttpEndpointOptions) (*HttpEndpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := fmt.Sprintf("http_endpoint-%d", len(m.httpEndpoints)+1)
	httpEndpoint := &HttpEndpoint{
		ID:         id,
		Name:       options.Name,
		BaseURL:    options.BaseURL,
		Headers:    options.Headers,
		AccountID:  "mock-account-id",
		InsertedAt: time.Now(),
		UpdatedAt:  time.Now(),
	}
	m.httpEndpoints[id] = httpEndpoint
	return httpEndpoint, nil
}

func (m *MockClient) GetHttpEndpoint(id string) (*HttpEndpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	httpEndpoint, ok := m.httpEndpoints[id]
	if !ok {
		return nil, fmt.Errorf("http endpoint not found")
	}
	return httpEndpoint, nil
}

func (m *MockClient) UpdateHttpEndpoint(id string, options *UpdateHttpEndpointOptions) (*HttpEndpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	httpEndpoint, ok := m.httpEndpoints[id]
	if !ok {
		return nil, fmt.Errorf("http endpoint not found")
	}

	if options.Name != nil {
		httpEndpoint.Name = *options.Name
	}
	if options.BaseURL != nil {
		httpEndpoint.BaseURL = *options.BaseURL
	}
	if options.Headers != nil {
		httpEndpoint.Headers = *options.Headers
	}
	httpEndpoint.UpdatedAt = time.Now()

	return httpEndpoint, nil
}

func (m *MockClient) DeleteHttpEndpoint(id string) (*DeleteSuccess, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.httpEndpoints[id]; !ok {
		return nil, fmt.Errorf("http endpoint not found")
	}
	delete(m.httpEndpoints, id)
	return &DeleteSuccess{ID: id, Deleted: true}, nil
}

func (m *MockClient) ListHttpEndpoints() ([]HttpEndpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	httpEndpoints := make([]HttpEndpoint, 0, len(m.httpEndpoints))
	for _, httpEndpoint := range m.httpEndpoints {
		httpEndpoints = append(httpEndpoints, *httpEndpoint)
	}
	return httpEndpoints, nil
}
