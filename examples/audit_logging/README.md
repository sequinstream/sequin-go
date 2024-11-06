# Sequin Audit Logging Example

This example demonstrates how to use Sequin to create audit logs for database changes. It processes changes from two tables (`user_permissions` and `subscriptions`) and stores them in corresponding audit log tables.

## Setup

1. Install dependencies:

```bash
go mod tidy
```

2. Run database migrations:

```bash
createdb audit_dev
migrate -path migrations -database "postgres://postgres:postgres@localhost:5432/dbname?sslmode=disable" up
```

## Running

```bash
go run cmd/audit_logger/main.go \
  -token="your-sequin-token" \
  -db-user="your-db-user" \
  -db-password="your-db-password" \
  -db-name="your-db-name"
```

## Configuration

The following flags are available:

- `-token`: Sequin API token (required)
- `-base-url`: Sequin API base URL (optional, defaults to "https://api.sequinstream.com/api")
- `-db-host`: Database host (default: "localhost")
- `-db-port`: Database port (default: 5432)
- `-db-user`: Database user (required)
- `-db-password`: Database password (required)
- `-db-name`: Database name (required)
- `-batch-size`: Maximum batch size for processing messages (default: 100)
- `-user-perm-consumer`: Consumer group for user permissions (default: "user-permissions-consumer")
- `-subs-consumer`: Consumer group for subscriptions (default: "subscriptions-consumer")

Example usage with custom base URL (e.g., for local development):

```bash
go run cmd/audit_logger/main.go \
  -token="your-sequin-token" \
  -base-url="http://localhost:7376" \
  -db-user="your-db-user" \
  -db-password="your-db-password" \
  -db-name="your-db-name" \
  -user-perm-consumer="custom-permissions-group" \
  -subs-consumer="custom-subscriptions-group"
```

## Architecture

- `cmd/audit_logger/main.go`: Main application entry point
- `internal/processor/processor.go`: Event processing logic
- `internal/db/db.go`: Database connection helper
- `migrations/`: Database schema migrations

The application:

1. Connects to your database
2. Consumes events from Sequin
3. Groups events by table
4. Processes each group in a transaction
5. Upserts events into the appropriate audit log table

Each audit log entry includes:

- The event ID (for idempotency)
- Relevant IDs (user_id, subscription_id, etc.)
- The action performed (insert, update, delete)
- Old and new values (as JSON)
- Timestamp
