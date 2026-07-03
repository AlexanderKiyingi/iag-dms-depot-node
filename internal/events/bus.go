// Package events publishes depot.* CloudEvents to iag.operations, optionally
// through a transactional outbox.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	SpecVersion = "1.0"
	Source      = "iag.dms-depot-node"
	TopicOps    = "iag.operations"

	TypeDocumentBuffered = "depot.document.buffered"
	TypeDocumentSynced   = "depot.document.synced"
	TypeNodeHeartbeat    = "depot.node.heartbeat"
)

type outboxEnqueuer interface {
	Enqueue(ctx context.Context, eventType, key string, payload any) error
}

type Bus struct {
	writer  *kafka.Writer
	enabled bool
	store   outboxEnqueuer
}

type Config struct {
	Brokers []string
	Enabled bool
}

func New(cfg Config) *Bus {
	if !cfg.Enabled || len(cfg.Brokers) == 0 {
		return &Bus{}
	}
	return &Bus{
		enabled: true,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        TopicOps,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Transport:    &kafka.Transport{ClientID: Source},
		},
	}
}

func (b *Bus) Enabled() bool { return b != nil && b.enabled }

func (b *Bus) SetOutbox(store outboxEnqueuer) {
	if b == nil {
		return
	}
	b.store = store
}

func (b *Bus) Close() error {
	if b == nil || b.writer == nil {
		return nil
	}
	return b.writer.Close()
}

type Envelope struct {
	SpecVersion string          `json:"specversion"`
	ID          string          `json:"id"`
	Source      string          `json:"source"`
	Type        string          `json:"type"`
	Data        json.RawMessage `json:"data"`
}

// Publish emits an event. When an outbox is configured, the event is enqueued
// transactionally and drained by the publisher; otherwise it is written to
// Kafka directly. Disabled buses drop events silently.
func (b *Bus) Publish(ctx context.Context, eventType, key string, data any) error {
	if b == nil || !b.enabled {
		return nil
	}
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	env := Envelope{
		SpecVersion: SpecVersion,
		ID:          uuid.NewString(),
		Source:      Source,
		Type:        eventType,
		Data:        body,
	}
	if key == "" {
		key = env.ID
	}
	if b.store != nil {
		if err := b.store.Enqueue(ctx, eventType, key, env); err != nil {
			slog.Warn("depot event enqueue failed", "type", eventType, "err", err)
		}
		return nil
	}
	return b.writeEnvelope(ctx, env, key)
}

// DispatchOutbox writes a pre-serialized outbox envelope to Kafka.
func (b *Bus) DispatchOutbox(ctx context.Context, eventType, eventKey string, payload []byte) error {
	if b == nil || !b.enabled || b.writer == nil {
		return nil
	}
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return fmt.Errorf("decode outbox payload: %w", err)
	}
	if env.Type == "" {
		env.Type = eventType
	}
	if env.ID == "" {
		env.ID = uuid.NewString()
	}
	if env.Source == "" {
		env.Source = Source
	}
	if env.SpecVersion == "" {
		env.SpecVersion = SpecVersion
	}
	key := eventKey
	if key == "" {
		key = env.ID
	}
	return b.writeEnvelope(ctx, env, key)
}

func (b *Bus) writeEnvelope(ctx context.Context, env Envelope, key string) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if err := b.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: raw,
		Headers: []kafka.Header{
			{Key: "ce-type", Value: []byte(env.Type)},
			{Key: "ce-source", Value: []byte(env.Source)},
		},
	}); err != nil {
		slog.Warn("kafka publish", "type", env.Type, "err", err)
		return err
	}
	return nil
}
