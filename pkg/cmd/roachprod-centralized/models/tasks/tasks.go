// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"log/slog"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TaskState represents the current execution state of a background task.
// Tasks progress through these states during their lifecycle: pending -> running -> done/failed.
type TaskState string

const (
	// TaskStatePending indicates a task is queued and waiting to be processed by a worker.
	TaskStatePending TaskState = "pending"
	// TaskStateRunning indicates a task is currently being executed by a worker.
	TaskStateRunning TaskState = "running"
	// TaskStateDone indicates a task completed successfully.
	TaskStateDone TaskState = "done"
	// TaskStateFailed indicates a task encountered an error and could not complete.
	TaskStateFailed TaskState = "failed"
)

// ITask defines the interface for background tasks in the roachprod-centralized system.
// All task implementations must provide these methods for lifecycle management and persistence.
// This interface enables polymorphic handling of different task types within the task processing system.
type ITask interface {
	// GetID returns the unique identifier for this task.
	GetID() uuid.UUID
	// SetID assigns a unique identifier to this task.
	SetID(uuid.UUID)
	// GetType returns a string identifying the task type (e.g., "cluster_sync", "dns_update").
	GetType() string
	// GetPayload returns the serialized task options/arguments as JSON bytes.
	GetPayload() []byte
	// SetPayload sets and deserializes the task options/arguments from JSON bytes.
	// Returns an error if the payload cannot be deserialized into the task's options struct.
	SetPayload([]byte) error
	// GetError returns the error message if the task failed, empty string otherwise.
	GetError() string
	// SetError sets the error message for a failed task.
	SetError(string)
	// GetCreationDatetime returns when this task was originally created.
	GetCreationDatetime() time.Time
	// SetCreationDatetime sets the task creation timestamp.
	SetCreationDatetime(time.Time)
	// GetUpdateDatetime returns when this task was last modified.
	GetUpdateDatetime() time.Time
	// SetUpdateDatetime sets the task last-modified timestamp.
	SetUpdateDatetime(time.Time)
	// GetState returns the current execution state of this task.
	GetState() TaskState
	// SetState updates the task's execution state.
	SetState(state TaskState)
	// AsLogAttributes returns task metadata as slog attributes for structured logging.
	// This method provides a consistent way to log tasks without exposing base64-encoded payloads.
	AsLogAttributes() []slog.Attr
}

// Task provides a basic implementation of the ITask interface.
// Specific task types should embed this struct and implement their own processing logic.
type Task struct {
	ID               uuid.UUID `json:"id" db:"id"`
	Type             string    `json:"type" db:"type"`
	State            TaskState `json:"state" db:"state"`
	Payload          []byte    `json:"payload,omitempty" db:"payload"` // Serialized task options/arguments as JSON
	Error            string    `json:"error,omitempty" db:"error"`     // Error message if task failed
	CreationDatetime time.Time `json:"creation_datetime" db:"creation_datetime"`
	UpdateDatetime   time.Time `json:"update_datetime" db:"update_datetime"`
}

// GetID returns the task ID.
func (t *Task) GetID() uuid.UUID {
	return t.ID
}

// SetID sets the task ID.
func (t *Task) SetID(id uuid.UUID) {
	t.ID = id
}

// GetType returns the task type.
func (t *Task) GetType() string {
	return t.Type
}

// GetPayload returns the task payload.
func (t *Task) GetPayload() []byte {
	return t.Payload
}

// SetPayload sets the task payload.
// Base Task implementation just stores the raw bytes.
// Tasks with options should override this to deserialize.
func (t *Task) SetPayload(data []byte) error {
	t.Payload = data
	return nil
}

// GetError returns the task error message.
func (t *Task) GetError() string {
	return t.Error
}

// SetError sets the task error message.
func (t *Task) SetError(err string) {
	t.Error = err
}

// GetState returns the task state.
func (t *Task) GetState() TaskState {
	return t.State
}

// SetState sets the task state.
func (t *Task) SetState(state TaskState) {
	t.State = state
}

// GetCreationDatetime returns the task creation datetime.
func (t *Task) GetCreationDatetime() time.Time {
	return t.CreationDatetime
}

// SetCreationDatetime sets the task creation datetime.
func (t *Task) SetCreationDatetime(ctime time.Time) {
	t.CreationDatetime = ctime
}

// GetUpdateDatetime returns the task update datetime.
func (t *Task) GetUpdateDatetime() time.Time {
	return t.UpdateDatetime
}

// SetUpdateDatetime sets the task update datetime.
func (t *Task) SetUpdateDatetime(utime time.Time) {
	t.UpdateDatetime = utime
}

// AsLogAttributes returns task metadata as slog attributes for structured logging.
// This method provides a consistent way to log tasks without exposing base64-encoded payloads.
func (t *Task) AsLogAttributes() []slog.Attr {
	return []slog.Attr{
		slog.String("task_id", t.ID.String()),
		slog.String("task_type", t.Type),
		slog.String("state", string(t.State)),
		slog.Time("created_at", t.CreationDatetime),
		slog.Time("updated_at", t.UpdateDatetime),
	}
}
