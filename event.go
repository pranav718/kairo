package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

type event struct {
	id        string            `json:"id"`
	eventtype string            `json:"type"`
	version   int               `json:"version"`
	source    string            `json:"source"`
	timestamp time.Time         `json:"timestamp"`
	data      json.RawMessage   `json:"data"`
	metadata  map[string]string `json:"metadata"`
}

type publisher struct {
	js     nats.JetStreamContext
	source string
}

func newpublisher(js nats.JetStreamContext, source string) *publisher {
	return &publisher{js: js, source: source}
}

func (p *publisher) publish(ctx context.Context, eventtype string, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal event data: %w", err)
	}

	evt := event{
		id:        uuid.NewString(),
		eventtype: eventtype,
		version:   1,
		source:    p.source,
		timestamp: time.Now().UTC(),
		data:      payload,
		metadata:  metadatafromcontext(ctx),
	}

	eventbytes, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	subject := fmt.Sprintf("events.%s", eventtype)
	_, err = p.js.Publish(subject, eventbytes)
	if err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	return nil
}

func metadatafromcontext(ctx context.Context) map[string]string {
	meta := make(map[string]string)

	if traceid, ok := ctx.Value("trace_id").(string); ok {
		meta["trace_id"] = traceid
	}
	if requestid, ok := ctx.Value("request_id").(string); ok {
		meta["request_id"] = requestid
	}

	return meta
}
