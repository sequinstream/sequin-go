package sequin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessor(t *testing.T) {
	t.Run("configuration", func(t *testing.T) {
		t.Run("validates options", func(t *testing.T) {
			tests := []struct {
				name string
				opts ProcessorOptions
				want error
			}{
				{
					name: "negative MaxBatchSize",
					opts: ProcessorOptions{MaxBatchSize: -1},
					want: errors.New("MaxBatchSize must be >= 0"),
				},
				{
					name: "negative MaxConcurrent",
					opts: ProcessorOptions{MaxConcurrent: -1},
					want: errors.New("MaxConcurrent must be >= 0"),
				},
				{
					name: "invalid prefetching",
					opts: ProcessorOptions{
						Prefetching: &PrefetchingOptions{BufferSize: 0},
					},
					want: errors.New("BufferSize must be > 0"),
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					client := newMockClient()
					handler := func(context.Context, []Message) error { return nil }

					_, err := NewProcessor(client, "test-group", handler, tt.opts)
					require.Error(t, err)
					assert.Contains(t, err.Error(), tt.want.Error())
				})
			}
		})

		t.Run("applies defaults", func(t *testing.T) {
			client := newMockClient()
			handler := func(context.Context, []Message) error { return nil }

			p, err := NewProcessor(client, "test-group", handler, ProcessorOptions{})
			require.NoError(t, err)

			assert.Equal(t, 1, p.opts.MaxBatchSize)
			assert.Equal(t, 1, p.opts.MaxConcurrent)
			assert.Equal(t, 1, p.opts.FetchBatchSize)
			assert.Nil(t, p.opts.Prefetching)
		})
	})

	t.Run("basic processing", func(t *testing.T) {
		t.Run("processes single message", func(t *testing.T) {
			client := newMockClient()
			processor := newTestProcessorFunc()

			msgs := generateTestMessages(1)
			client.setMessages(msgs)

			p, err := NewProcessor(client, "test-group", processor.handler, ProcessorOptions{
				MaxBatchSize: 1,
			})
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			// Create a channel to signal when processing is complete
			done := make(chan error, 1)
			go func() {
				done <- p.Run(ctx)
			}()

			// Wait for either processing to complete or timeout
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(500 * time.Millisecond):
				t.Fatal("processor did not complete in time")
			}

			// Verify the message was processed
			processed := processor.processedMessages()
			require.Len(t, processed, 1)
			require.Equal(t, msgs, processed[0])

			// Verify the message was acknowledged
			acked := client.acknowledgedMessages()
			require.Len(t, acked, 1)
			require.Equal(t, msgs[0].AckID, acked[0])
		})

		t.Run("processes batches", func(t *testing.T) {
			client := newMockClient()
			processor := newTestProcessorFunc()

			msgs := generateTestMessages(25) // More than MaxBatchSize
			client.setMessages(msgs)

			p, err := NewProcessor(client, "test-group", processor.handler, ProcessorOptions{
				MaxBatchSize: 10,
			})
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				errCh <- p.Run(ctx)
			}()

			// Wait for processing
			time.Sleep(50 * time.Millisecond)
			cancel()

			require.NoError(t, <-errCh)

			processed := processor.processedMessages()

			// Verify batch sizes
			var totalProcessed int
			for _, batch := range processed {
				assert.LessOrEqual(t, len(batch), 10)
				totalProcessed += len(batch)
			}
			assert.Equal(t, 25, totalProcessed)

			// Verify all messages were acknowledged
			acked := client.acknowledgedMessages()
			assert.Len(t, acked, 25)
		})
	})

	t.Run("concurrent processing", func(t *testing.T) {
		client := newMockClient()
		processor := newTestProcessorFunc()
		processor.processDelay = 10 * time.Millisecond

		msgs := generateTestMessages(50)
		client.setMessages(msgs)

		p, err := NewProcessor(client, "test-group", processor.handler, ProcessorOptions{
			MaxBatchSize:  5,
			MaxConcurrent: 3,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		start := time.Now()
		errCh := make(chan error, 1)
		go func() {
			errCh <- p.Run(ctx)
		}()

		time.Sleep(50 * time.Millisecond)
		cancel()

		require.NoError(t, <-errCh)
		duration := time.Since(start)

		// With 50 messages, batch size 5, and 3 concurrent processors,
		// it should take ~20ms (2 rounds of processing)
		assert.Less(t, duration, 60*time.Millisecond)

		processed := processor.processedMessages()
		var totalProcessed int
		for _, batch := range processed {
			totalProcessed += len(batch)
		}
		assert.Equal(t, 50, totalProcessed)
	})

	t.Run("prefetching", func(t *testing.T) {
		t.Run("buffers messages", func(t *testing.T) {
			client := newMockClient()
			processor := newTestProcessorFunc()
			processor.processDelay = 10 * time.Millisecond

			msgs := generateTestMessages(100)
			client.setMessages(msgs)

			p, err := NewProcessor(client, "test-group", processor.handler, ProcessorOptions{
				MaxBatchSize:  10,
				MaxConcurrent: 2,
				Prefetching: &PrefetchingOptions{
					BufferSize: 30,
				},
			})
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				errCh <- p.Run(ctx)
			}()

			// Wait for initial prefetch
			time.Sleep(20 * time.Millisecond)

			// Check received batch sizes
			sizes := client.receivedBatchSizes()
			assert.NotEmpty(t, sizes)

			// Should have made multiple receive calls to fill buffer
			assert.True(t, len(sizes) > 1)

			cancel()
			require.NoError(t, <-errCh)
		})
	})

	t.Run("error handling", func(t *testing.T) {
		t.Run("handles processor errors", func(t *testing.T) {
			client := newMockClient()
			processor := newTestProcessorFunc()
			processor.err = errors.New("processing failed")

			var errorHandlerCalled bool
			errorHandler := func(_ context.Context, msgs []Message, err error) {
				errorHandlerCalled = true
				assert.EqualError(t, err, "handler failed: processing failed")
			}

			msgs := generateTestMessages(1)
			client.setMessages(msgs)

			p, err := NewProcessor(client, "test-group", processor.handler, ProcessorOptions{
				MaxBatchSize: 1,
				ErrorHandler: errorHandler,
			})
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				errCh <- p.Run(ctx)
			}()

			time.Sleep(50 * time.Millisecond)
			cancel()

			require.NoError(t, <-errCh)
			assert.True(t, errorHandlerCalled)

			// Message should not have been acknowledged
			acked := client.acknowledgedMessages()
			assert.Empty(t, acked)
		})

		t.Run("handles client errors", func(t *testing.T) {
			client := newMockClient()
			client.receiveErr = errors.New("receive failed")
			processor := newTestProcessorFunc()

			var errorHandlerCalled bool
			errorHandler := func(_ context.Context, msgs []Message, err error) {
				errorHandlerCalled = true
				assert.Contains(t, err.Error(), "receive failed")
			}

			p, err := NewProcessor(client, "test-group", processor.handler, ProcessorOptions{
				MaxBatchSize: 1,
				ErrorHandler: errorHandler,
			})
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				errCh <- p.Run(ctx)
			}()

			time.Sleep(50 * time.Millisecond)
			cancel()

			require.NoError(t, <-errCh)
			assert.True(t, errorHandlerCalled)
		})
	})

	t.Run("shutdown", func(t *testing.T) {
		t.Run("completes in-flight messages", func(t *testing.T) {
			client := newMockClient()
			processor := newTestProcessorFunc()
			processor.processDelay = 50 * time.Millisecond

			msgs := generateTestMessages(10)
			client.setMessages(msgs)

			p, err := NewProcessor(client, "test-group", processor.handler, ProcessorOptions{
				MaxBatchSize:  5,
				MaxConcurrent: 2,
			})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())

			errCh := make(chan error, 1)
			go func() {
				errCh <- p.Run(ctx)
			}()

			// Wait for processing to start
			time.Sleep(20 * time.Millisecond)

			// Cancel context and wait for shutdown
			cancel()

			// Should complete without error
			require.NoError(t, <-errCh)

			// Check that messages that were in-flight were completed
			processed := processor.processedMessages()
			acked := client.acknowledgedMessages()

			assert.NotEmpty(t, processed)
			assert.NotEmpty(t, acked)
		})

		t.Run("drains prefetch buffer", func(t *testing.T) {
			client := newMockClient()
			processor := newTestProcessorFunc()

			msgs := generateTestMessages(20)
			client.setMessages(msgs)

			p, err := NewProcessor(client, "test-group", processor.handler, ProcessorOptions{
				MaxBatchSize:  5,
				MaxConcurrent: 2,
				Prefetching: &PrefetchingOptions{
					BufferSize: 10,
				},
			})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())

			errCh := make(chan error, 1)
			go func() {
				errCh <- p.Run(ctx)
			}()

			// Wait for buffer to fill
			time.Sleep(50 * time.Millisecond)

			// Cancel context
			cancel()

			require.NoError(t, <-errCh)

			// Verify messages in buffer were processed
			processed := processor.processedMessages()
			var totalProcessed int
			for _, batch := range processed {
				totalProcessed += len(batch)
			}
			assert.True(t, totalProcessed >= 10, "Should process at least buffered messages")
		})

		t.Run("stops receiving after shutdown", func(t *testing.T) {
			client := newMockClient()
			client.receiveDelay = 10 * time.Millisecond
			processor := newTestProcessorFunc()

			msgs := generateTestMessages(100)
			client.setMessages(msgs)

			p, err := NewProcessor(client, "test-group", processor.handler, ProcessorOptions{
				MaxBatchSize:  5,
				MaxConcurrent: 2,
			})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())

			errCh := make(chan error, 1)
			go func() {
				errCh <- p.Run(ctx)
			}()

			// Let it process some messages
			time.Sleep(30 * time.Millisecond)

			// Get current receive count
			initialReceiveCount := client.receiveCount

			// Cancel and wait for shutdown
			cancel()
			require.NoError(t, <-errCh)

			// Wait a bit to ensure no more receives
			time.Sleep(20 * time.Millisecond)

			// Should have minimal additional receives during shutdown
			finalReceiveCount := client.receiveCount
			assert.Less(t, finalReceiveCount-initialReceiveCount, 3,
				"Should not make many new receives during shutdown")
		})
	})

	t.Run("stress test", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping stress test in short mode")
		}

		client := newMockClient()
		processor := newTestProcessorFunc()
		processor.processDelay = 1 * time.Millisecond

		msgs := generateTestMessages(1000)
		client.setMessages(msgs)

		p, err := NewProcessor(client, "test-group", processor.handler, ProcessorOptions{
			MaxBatchSize:  20,
			MaxConcurrent: 5,
			Prefetching: &PrefetchingOptions{
				BufferSize: 100,
			},
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		start := time.Now()
		err = p.Run(ctx)
		duration := time.Since(start)

		require.ErrorIs(t, err, context.DeadlineExceeded)

		processed := processor.processedMessages()
		var totalProcessed int
		for _, batch := range processed {
			totalProcessed += len(batch)
			assert.LessOrEqual(t, len(batch), 20, "Batch size limit respected")
		}

		t.Logf("Processed %d messages in %v", totalProcessed, duration)
		assert.GreaterOrEqual(t, totalProcessed, 500, "Should process many messages")

		// Verify acknowledgments
		acked := client.acknowledgedMessages()
		assert.Equal(t, len(acked), totalProcessed, "All processed messages should be acknowledged")
	})
}
