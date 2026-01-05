// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"encoding/json"
	"log/slog"

	"github.com/cockroachdb/errors"
)

// TaskWithOptions is a generic helper that provides automatic JSON marshaling
// for task options. Task implementations should embed this instead of Task directly
// when they need options/arguments.
//
// Example usage:
//
//	type CreateClusterOptions struct {
//	    NodeCount    int    `json:"node_count"`
//	    MachineType  string `json:"machine_type"`
//	}
//
//	type TaskCreateCluster struct {
//	    tasks.TaskWithOptions[CreateClusterOptions]
//	    Service models.IService
//	}
//
//	func NewTaskCreateCluster(opts CreateClusterOptions) (*TaskCreateCluster, error) {
//	    task := &TaskCreateCluster{
//	        TaskWithOptions: tasks.TaskWithOptions[CreateClusterOptions]{
//	            Task: tasks.Task{Type: "clusters_create"},
//	        },
//	    }
//	    if err := task.SetOptions(opts); err != nil {
//	        return nil, err
//	    }
//	    return task, nil
//	}
//
//	func (t *TaskCreateCluster) Process(ctx context.Context, l *logger.Logger) error {
//	    opts := t.GetOptions()  // Type-safe access!
//	    // Use opts.NodeCount, opts.MachineType, etc.
//	    return t.Service.Create(ctx, l, opts)
//	}
type TaskWithOptions[T any] struct {
	Task      // Embed base task
	Options T `json:"-"` // The typed options (not directly serialized to avoid duplication)
}

// GetOptions returns the typed options for this task.
func (t *TaskWithOptions[T]) GetOptions() *T {
	return &t.Options
}

// SetOptions sets the typed options and automatically marshals them to the Payload field.
// Returns an error if the options cannot be marshaled to JSON (should be very rare -
// only happens if options contain unmarshalable types like channels or functions).
func (t *TaskWithOptions[T]) SetOptions(opts T) error {
	t.Options = opts

	// Auto-marshal to JSON
	data, err := json.Marshal(opts)
	if err != nil {
		return errors.Wrap(err, "failed to marshal task options to JSON")
	}

	t.Payload = data
	return nil
}

// SetPayload deserializes the payload into typed Options automatically.
// This method is called by the repository when loading tasks from the database.
//
// Error cases:
//   - Invalid JSON: The payload is corrupted or not valid JSON
//   - Schema mismatch: The payload doesn't match the current Options struct
//     (e.g., required field was added, field type changed, field renamed)
//   - Type conversion errors: JSON types don't match Go types
//
// Callers MUST handle errors - a task with invalid payload cannot be processed.
func (t *TaskWithOptions[T]) SetPayload(data []byte) error {
	if len(data) == 0 {
		// Empty payload is valid (no options provided)
		var zero T
		t.Options = zero
		t.Payload = nil
		return nil
	}

	// Auto-unmarshal from JSON
	if err := json.Unmarshal(data, &t.Options); err != nil {
		return errors.Wrapf(err,
			"failed to unmarshal task options from JSON (task_type=%s, payload_size=%d)",
			t.Type, len(data))
	}

	t.Payload = data
	return nil
}

// GetPayload returns the serialized payload (already set by SetOptions).
func (t *TaskWithOptions[T]) GetPayload() []byte {
	return t.Payload
}

// AsLogAttributes returns task metadata including deserialized options for structured logging.
// This override provides the same base metadata as Task.AsLogAttributes() plus the typed options,
// avoiding the need to log base64-encoded payloads.
func (t *TaskWithOptions[T]) AsLogAttributes() []slog.Attr {
	// Get base attributes from embedded Task
	attrs := t.Task.AsLogAttributes()

	// Add deserialized options
	attrs = append(attrs, slog.Any("options", t.Options))

	return attrs
}
