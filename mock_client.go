package sequin

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// mockClient implements a controllable test double for Client
type mockClient struct {
	mu sync.Mutex

	// Tracks calls to methods
	receiveCount      int
	receiveBatchSizes []int
	ackCount          int

	// Messages to return from Receive
	messages   []Message
	messageIdx int

	// Records which messages were acknowledged
	ackedMessages map[string]bool

	// For controlling behavior
	receiveDelay time.Duration
	receiveErr   error
	ackErr       error
}

func newMockClient() *mockClient {
	return &mockClient{
		ackedMessages: make(map[string]bool),
	}
}

func (m *mockClient) Receive(ctx context.Context, consumerGroupID string, params *ReceiveParams) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.receiveCount++
	if params != nil {
		m.receiveBatchSizes = append(m.receiveBatchSizes, params.MaxBatchSize)
	}

	// Return no more messages after all messages have been delivered
	if m.messageIdx >= len(m.messages) {
		return nil, nil
	}

	// Get batch size
	batchSize := 1
	if params != nil && params.MaxBatchSize > 0 {
		batchSize = params.MaxBatchSize
	}

	// Calculate end index
	end := m.messageIdx + batchSize
	if end > len(m.messages) {
		end = len(m.messages)
	}

	// Get messages
	batch := make([]Message, end-m.messageIdx)
	copy(batch, m.messages[m.messageIdx:end])
	m.messageIdx = end

	return batch, nil
}

func (m *mockClient) Ack(ctx context.Context, consumerGroupID string, ackIDs []string) error {
	if m.ackErr != nil {
		return m.ackErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.ackCount++
	for _, id := range ackIDs {
		m.ackedMessages[id] = true
	}

	return nil
}

// Helper functions for tests
func (m *mockClient) setMessages(msgs []Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = msgs
	m.messageIdx = 0
}

func (m *mockClient) acknowledgedMessages() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var acked []string
	for id, wasAcked := range m.ackedMessages {
		if wasAcked {
			acked = append(acked, id)
		}
	}
	sort.Strings(acked)
	return acked
}

func (m *mockClient) receivedBatchSizes() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int{}, m.receiveBatchSizes...)
}

// generateTestMessages creates n test messages
func generateTestMessages(n int) []Message {
	msgs := make([]Message, n)
	for i := range msgs {
		msgs[i] = Message{
			AckID:  fmt.Sprintf("msg-%d", i),
			Record: []byte(fmt.Sprintf(`{"value": %d}`, i)),
		}
	}
	return msgs
}

// testProcessorFunc creates a ProcessorFunc that tracks processed messages
type testProcessorFunc struct {
	mu           sync.Mutex
	processed    [][]Message // Each batch of messages processed
	processDelay time.Duration
	err          error
}

func newTestProcessorFunc() *testProcessorFunc {
	return &testProcessorFunc{}
}

func (t *testProcessorFunc) handler(ctx context.Context, msgs []Message) error {
	if t.processDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.processDelay):
		}
	}

	if t.err != nil {
		return t.err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	batch := make([]Message, len(msgs))
	copy(batch, msgs)
	t.processed = append(t.processed, batch)

	return nil
}

func (t *testProcessorFunc) processedMessages() [][]Message {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make([][]Message, len(t.processed))
	for i, batch := range t.processed {
		batchCopy := make([]Message, len(batch))
		copy(batchCopy, batch)
		result[i] = batchCopy
	}
	return result
}

// Ensure mockClient implements SequinClient interface
var _ SequinClient = (*mockClient)(nil)

// Add Nack method to satisfy interface
func (m *mockClient) Nack(ctx context.Context, consumerGroupID string, ackIDs []string) error {
	// Implementation for tests if needed
	return nil
}
