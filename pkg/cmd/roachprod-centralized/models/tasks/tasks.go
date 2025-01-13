// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type TaskState string

const (
	// TaskStatePending is the state of a task that is waiting.
	TaskStatePending TaskState = "pending"
	// TaskStateRunning is the state of a task that is running.
	TaskStateRunning TaskState = "running"
	// TaskStateDone is the state of a task that is done.
	TaskStateDone TaskState = "done"
	// TaskStateFailed is the state of a task that has failed.
	TaskStateFailed TaskState = "failed"
)

// ITask is the interface for a task.
type ITask interface {
	GetID() uuid.UUID
	SetID(uuid.UUID)
	GetType() string
	GetCreationDatetime() time.Time
	SetCreationDatetime(time.Time)
	GetUpdateDatetime() time.Time
	SetUpdateDatetime(time.Time)
	GetState() TaskState
	SetState(state TaskState)
}

// Task is the struct for a task.
type Task struct {
	ID               uuid.UUID
	Type             string
	State            TaskState
	CreationDatetime time.Time
	UpdateDatetime   time.Time
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
