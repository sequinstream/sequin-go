# `sequin-go`

A Go SDK for consuming messages from a consumer group in [Sequin](https://github.com/sequinstream/sequin).

See the [docs on pkg.go.dev](https://pkg.go.dev/github.com/sequinstream/sequin-go@v0.1.0#section-documentation).

## Installing

Install the library:

```shell
go get github.com/sequinstream/sequin-go
```

## Usage

The `sequin-go` SDK provides a high-level `Processor` for consuming messages from Sequin consumer groups. Here's a simple example:

```go
// Initialize the client
client := sequin.NewClient(&sequin.ClientOptions{
    Token: "your-token",
})

// Create a processor
processor, err := sequin.NewProcessor(
    client,
    "your-consumer-group",
    func(ctx context.Context, msgs []sequin.Message) error {
        // Process each message in the batch
        for _, msg := range msgs {
            fmt.Printf("Received message: %s\n", string(msg.Record))
        }
        return nil
    },
    sequin.ProcessorOptions{
        MaxBatchSize: 10,           // Process up to 10 messages at once
        MaxConcurrent: 3,           // Run up to 3 processors concurrently
        FetchBatchSize: 20,         // Fetch 20 messages at a time from server
        Prefetching: &sequin.PrefetchingOptions{
            BufferSize: 100,        // Buffer up to 100 messages
        },
    },
)
if err != nil {
    log.Fatal(err)
}

// Run the processor (blocks until context is cancelled)
ctx := context.Background()
if err := processor.Run(ctx); err != nil {
    log.Fatal(err)
}
```

### Configuration

The `Processor` supports several configuration options:

- `MaxBatchSize`: Maximum number of messages to process in a single batch
- `MaxConcurrent`: Maximum number of concurrent batch processors
- `FetchBatchSize`: Number of messages to request from server in a single call
- `Prefetching`: Optional message prefetching configuration
  - `BufferSize`: How many messages to prefetch and buffer

### Examples

For complete working examples, see:

- [Simple consumer](examples/simple/main.go) - Basic message processing to stdout/file
- [Audit logging](examples/audit_logging/main.go) - More complex example with database integration

### Environment Variables

By default, the Client connects to Sequin Cloud at `https://api.sequinstream.com`. To use a local Sequin instance, set:

```shell
export SEQUIN_URL=http://localhost:7376
```
