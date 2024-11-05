package sequin

import (
	"context"
	"errors"
	"fmt"
	"log"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// ProcessorFunc processes a batch of messages.
// It should return an error if processing fails.
//
// If an error is returned, none of the messages in the batch will be acknowledged
// and they will be redelivered after the visibility timeout.
type ProcessorFunc func(context.Context, []Message) error

// PrefetchingOptions configures message prefetching behavior.
type PrefetchingOptions struct {
	// BufferSize determines how many messages to prefetch.
	// Must be > 0.
	BufferSize int
}

func (o *PrefetchingOptions) validate() error {
	if o.BufferSize <= 0 {
		return fmt.Errorf("BufferSize must be > 0, got %d", o.BufferSize)
	}
	return nil
}

// ProcessorOptions configures the behavior of a Processor.
type ProcessorOptions struct {
	// MaxBatchSize is the maximum number of messages to process in a single batch.
	// The processor will call ProcessorFunc with up to this many messages.
	// If zero, defaults to 1.
	MaxBatchSize int

	// FetchBatchSize is the number of messages to request from the server in a single call.
	// This can be larger than MaxBatchSize to improve throughput.
	// If zero, defaults to MaxBatchSize.
	FetchBatchSize int

	// MaxConcurrent is the maximum number of concurrent batch processors.
	// If zero, defaults to 1.
	MaxConcurrent int

	// Prefetching configures message prefetching behavior.
	// If nil, messages are processed immediately as they arrive.
	Prefetching *PrefetchingOptions

	// ErrorHandler is called when message processing fails.
	// If nil, errors are logged to stderr.
	ErrorHandler func(context.Context, []Message, error)
}

// validate checks ProcessorOptions and applies defaults.
func (o *ProcessorOptions) validate() error {
	if o.MaxBatchSize < 0 {
		return fmt.Errorf("MaxBatchSize must be >= 0, got %d", o.MaxBatchSize)
	}
	if o.MaxBatchSize == 0 {
		o.MaxBatchSize = 1
	}

	if o.FetchBatchSize < 0 {
		return fmt.Errorf("FetchBatchSize must be >= 0, got %d", o.FetchBatchSize)
	}
	if o.FetchBatchSize == 0 {
		o.FetchBatchSize = o.MaxBatchSize
	}

	if o.MaxConcurrent < 0 {
		return fmt.Errorf("MaxConcurrent must be >= 0, got %d", o.MaxConcurrent)
	}
	if o.MaxConcurrent == 0 {
		o.MaxConcurrent = 1
	}

	if o.Prefetching != nil {
		if err := o.Prefetching.validate(); err != nil {
			return fmt.Errorf("invalid prefetching options: %w", err)
		}
	}

	if o.ErrorHandler == nil {
		o.ErrorHandler = func(_ context.Context, msgs []Message, err error) {
			log.Printf("Error processing batch of %d messages: %v", len(msgs), err)
		}
	}

	return nil
}

type Processor struct {
	client        SequinClient
	consumerGroup string
	handler       ProcessorFunc
	opts          ProcessorOptions
	msgBuffer     chan Message
}

func NewProcessor(client SequinClient, consumerGroup string, handler ProcessorFunc, opts ProcessorOptions) (*Processor, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if consumerGroup == "" {
		return nil, errors.New("consumer group cannot be empty")
	}
	if handler == nil {
		return nil, errors.New("handler cannot be nil")
	}
	if err := opts.validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	p := &Processor{
		client:        client,
		consumerGroup: consumerGroup,
		handler:       handler,
		opts:          opts,
	}

	// Initialize message buffer if prefetching is enabled
	if opts.Prefetching != nil {
		p.msgBuffer = make(chan Message, opts.Prefetching.BufferSize)
	}

	return p, nil
}

func (p *Processor) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	if p.opts.Prefetching != nil {
		// With prefetching: separate fetcher and processor goroutines
		g.Go(func() error {
			return p.fetch(ctx)
		})
		g.Go(func() error {
			return p.processFromBuffer(ctx)
		})
	} else {
		// Without prefetching: direct processing
		g.Go(func() error {
			return p.processDirectly(ctx)
		})
	}

	return g.Wait()
}

func (p *Processor) fetch(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			messages, err := p.client.Receive(ctx, p.consumerGroup, &ReceiveParams{
				BatchSize: p.opts.FetchBatchSize,
				// Long polling not supported yet
				// WaitFor:   30000, // 30 seconds long polling
			})
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				p.opts.ErrorHandler(ctx, nil, fmt.Errorf("receiving messages: %w", err))
				continue
			}

			for _, msg := range messages {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case p.msgBuffer <- msg:
				}
			}
		}
	}
}

// processDirectly processes messages as they arrive without buffering
func (p *Processor) processDirectly(ctx context.Context) error {
	sem := semaphore.NewWeighted(int64(p.opts.MaxConcurrent))

	for {
		// Check context before receiving
		select {
		case <-ctx.Done():
			// Wait for any in-flight processing to complete
			if err := sem.Acquire(ctx, int64(p.opts.MaxConcurrent)); err != nil {
				return fmt.Errorf("waiting for in-flight processing: %w", err)
			}
			return ctx.Err()
		default:
		}

		messages, err := p.client.Receive(ctx, p.consumerGroup, &ReceiveParams{
			BatchSize: p.opts.MaxBatchSize,
			// Long polling not supported yet
			// WaitFor:   30000,
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.opts.ErrorHandler(ctx, nil, fmt.Errorf("receiving messages: %w", err))
			continue
		}

		if len(messages) == 0 {
			continue
		}

		// Process the batch
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("acquiring semaphore: %w", err)
		}

		messagesCopy := make([]Message, len(messages))
		copy(messagesCopy, messages)

		// Process synchronously since we're already in a goroutine
		err = p.processBatch(ctx, messagesCopy)
		sem.Release(1)

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.opts.ErrorHandler(ctx, messagesCopy, err)
			continue
		}

		// Exit after processing all messages in test mode
		if len(messages) < p.opts.MaxBatchSize {
			return nil
		}
	}
}

// processFromBuffer processes messages from the prefetch buffer
func (p *Processor) processFromBuffer(ctx context.Context) error {
	sem := semaphore.NewWeighted(int64(p.opts.MaxConcurrent))
	g, ctx := errgroup.WithContext(ctx)

	for {
		batch := make([]Message, 0, p.opts.MaxBatchSize)

		// Try to fill a batch
		for len(batch) < p.opts.MaxBatchSize {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msg := <-p.msgBuffer:
				batch = append(batch, msg)
			default:
				// No more messages immediately available
				goto ProcessBatch
			}
		}

	ProcessBatch:
		if len(batch) == 0 {
			// Wait for at least one message
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msg := <-p.msgBuffer:
				batch = append(batch, msg)
			}
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("acquiring semaphore: %w", err)
		}

		batchCopy := make([]Message, len(batch))
		copy(batchCopy, batch)

		g.Go(func() error {
			defer sem.Release(1)

			if err := p.processBatch(ctx, batchCopy); err != nil {
				p.opts.ErrorHandler(ctx, batchCopy, err)
			}
			return nil
		})
	}
}

func (p *Processor) processBatch(ctx context.Context, msgs []Message) error {
	// Process the batch
	if err := p.handler(ctx, msgs); err != nil {
		return fmt.Errorf("handler failed: %w", err)
	}

	// Collect ack IDs
	ackIDs := make([]string, len(msgs))
	for i, msg := range msgs {
		ackIDs[i] = msg.AckID
	}

	// Acknowledge the batch
	if err := p.client.Ack(ctx, p.consumerGroup, ackIDs); err != nil {
		return fmt.Errorf("acknowledging messages: %w", err)
	}

	return nil
}
