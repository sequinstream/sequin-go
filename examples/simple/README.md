# Simple message consumer

This example demonstrates a basic Sequin consumer that writes messages to stdout or a file.

## Running the example

Output to stdout:

```bash
go run main.go --token=your-sequin-api-token --consumer-group=your-consumer-group-name-or-id
```

Output to file:

```bash
go run main.go --token=your-sequin-api-token --consumer-group=your-consumer-group-name-or-id --output=messages.log
```

### Using a different API endpoint

If you're running Sequin locally or using a different deployment, you can specify the base URL:

```bash
go run main.go \
    --token=your-sequin-api-token \
    --consumer-group=your-consumer-group-name-or-id \
    --base-url=http://localhost:7376
```

## Code overview / how it works

The consumer is built around four main components:

1. **Configuration**: Command-line flags for customization
2. **Output Setup**: Configurable output to either stdout or a file
3. **Message Processing**: A simple processor that writes messages
4. **Graceful Shutdown**: Signal handling for clean termination

### Configuration

The program accepts several command-line flags:

```go
token := flag.String("token", "", "Sequin API token")
consumerGroup := flag.String("consumer-group", "", "Consumer Group name or ID")
outputFile := flag.String("output", "", "Output file path (optional, defaults to stdout)")
batchSize := flag.Int("batch-size", 10, "Maximum batch size for processing messages")
baseURL := flag.String("base-url", "", "Sequin API base URL (optional, defaults to https://api.sequinstream.com/api)")
```

### Output setup

The program can write to either stdout (default) or a file:

```go
if *outputFile != "" {
    output, err = os.OpenFile(*outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    // ... error handling ...
} else {
    output = os.Stdout
}
```

### Message processing

A simple processor writes each message to the configured output:

```go
processor, err := sequin.NewProcessor(
    client,
    *consumerGroup,
    func(ctx context.Context, msgs []sequin.Message) error {
        for _, msg := range msgs {
            fmt.Fprintf(output, "%s\n", string(msg.Record))
        }
        return nil
    },
    sequin.ProcessorOptions{
        MaxBatchSize: *batchSize,
    },
)
```

### Graceful shutdown

The program handles SIGINT and SIGTERM signals for clean shutdown:

```go
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
go func() {
    <-sigChan
    log.Println("Shutting down...")
    cancel()
}()
```

## Configuration options

| Flag               | Description                                | Default                          |
| ------------------ | ------------------------------------------ | -------------------------------- |
| `--token`          | Sequin API token (required)                | -                                |
| `--consumer-group` | Consumer Group ID (required)               | -                                |
| `--output`         | Output file path                           | stdout                           |
| `--batch-size`     | Maximum batch size for processing messages | 10                               |
| `--base-url`       | Sequin API base URL                        | https://api.sequinstream.com/api |
