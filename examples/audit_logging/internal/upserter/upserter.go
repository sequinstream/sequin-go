package upserter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/jackc/pgx/v4"
)

// Add this custom type to handle both string and number IDs
type ID string

func (id *ID) UnmarshalJSON(data []byte) error {
	// Try string first
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*id = ID(s)
		return nil
	}

	// Try number
	var n json.Number
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	*id = ID(n.String())
	return nil
}

type AuditEvent struct {
	ID        ID              `json:"id"`
	TableName string          `json:"source_table_name"`
	Action    string          `json:"action"`
	Record    json.RawMessage `json:"record"`
	OldRecord json.RawMessage `json:"old_record,omitempty"`
}

type TableConfig struct {
	TableName     string
	ConsumerGroup string
	ProcessFunc   func(*AuditUpserter, context.Context, pgx.Tx, []AuditEvent) error
}

type AuditUpserter struct {
	db      *pgxpool.Pool
	configs []TableConfig
}

func New(db *pgxpool.Pool, userPermConsumer, subscriptionsConsumer string) *AuditUpserter {
	return &AuditUpserter{
		db: db,
		configs: []TableConfig{
			{
				TableName:     "user_permissions",
				ConsumerGroup: userPermConsumer,
				ProcessFunc:   (*AuditUpserter).processUserPermissionEvents,
			},
			{
				TableName:     "subscriptions",
				ConsumerGroup: subscriptionsConsumer,
				ProcessFunc:   (*AuditUpserter).processSubscriptionEvents,
			},
		},
	}
}

func (p *AuditUpserter) ProcessTableEvents(ctx context.Context, events []AuditEvent) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := p.db.Begin(ctx)
	if err != nil {
		log.Printf("Error beginning transaction: %v", err)
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Find the correct process function for this table
	tableName := events[0].TableName
	var processed bool
	for _, cfg := range p.configs {
		if cfg.TableName == tableName {
			log.Printf("Processing %d events for table %s", len(events), tableName)
			if err := cfg.ProcessFunc(p, ctx, tx, events); err != nil {
				log.Printf("Error processing %s events: %v", tableName, err)
				return fmt.Errorf("processing %s events: %w", tableName, err)
			}
			processed = true
			break
		}
	}

	if !processed {
		log.Printf("Warning: No processor found for table %s", tableName)
		return fmt.Errorf("no processor found for table %s", tableName)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Error committing transaction: %v", err)
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.Printf("Successfully committed %d events for table %s", len(events), tableName)
	return nil
}

func (p *AuditUpserter) processUserPermissionEvents(ctx context.Context, tx pgx.Tx, events []AuditEvent) error {
	log.Printf("Processing %d user permission events", len(events))
	sql := `
		insert into user_permissions_log (
			event_id, user_id, permission, action, old_values, new_values
		)
		values ($1, $2, $3, $4, $5, $6)
		on conflict (event_id) do update set
			user_id = excluded.user_id,
			permission = excluded.permission,
			action = excluded.action,
			old_values = excluded.old_values,
			new_values = excluded.new_values
	`

	batch := &pgx.Batch{}
	for _, event := range events {
		var newRecord map[string]interface{}
		if err := json.Unmarshal(event.Record, &newRecord); err != nil {
			log.Printf("Error unmarshaling user permission record %s: %v", event.ID, err)
			return fmt.Errorf("unmarshaling record: %w", err)
		}

		var oldValues interface{} = nil
		if event.OldRecord != nil && len(event.OldRecord) > 0 {
			oldValues = event.OldRecord
		}

		batch.Queue(sql,
			string(event.ID),
			newRecord["user_id"],
			newRecord["permission"],
			event.Action,
			oldValues,
			event.Record,
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("executing batch row %d: %w", i, err)
		}
	}

	log.Printf("Successfully processed user permission events batch")
	return nil
}

func (p *AuditUpserter) processSubscriptionEvents(ctx context.Context, tx pgx.Tx, events []AuditEvent) error {
	log.Printf("Processing %d subscription events", len(events))
	sql := `
		insert into subscriptions_log (
			event_id, subscription_id, customer_id, status, action, old_values, new_values
		)
		values ($1, $2, $3, $4, $5, $6, $7)
		on conflict (event_id) do update set
			subscription_id = excluded.subscription_id,
			customer_id = excluded.customer_id,
			status = excluded.status,
			action = excluded.action,
			old_values = excluded.old_values,
			new_values = excluded.new_values
	`

	batch := &pgx.Batch{}
	for _, event := range events {
		var newRecord map[string]interface{}
		if err := json.Unmarshal(event.Record, &newRecord); err != nil {
			log.Printf("Error unmarshaling subscription record %s: %v", event.ID, err)
			return fmt.Errorf("unmarshaling record: %w", err)
		}

		var oldValues interface{} = nil
		if event.OldRecord != nil && len(event.OldRecord) > 0 {
			oldValues = event.OldRecord
		}

		batch.Queue(sql,
			string(event.ID),
			fmt.Sprint(newRecord["id"]),
			newRecord["customer_id"],
			newRecord["status"],
			event.Action,
			oldValues,
			event.Record,
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("executing batch row %d: %w", i, err)
		}
	}

	log.Printf("Successfully processed subscription events batch")
	return nil
}

func (p *AuditUpserter) GetConfigs() []TableConfig {
	return p.configs
}
