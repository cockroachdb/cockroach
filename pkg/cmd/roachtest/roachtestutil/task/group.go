// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package task

// Group is an interface for managing a group of tasks. It is intended for use
// in roachtests, for creating a group and waiting for all tasks in the group to
// complete.
type Group interface {
	Tasker
	// Wait waits for all tasks in the group to complete. Errors from tasks are
	// reported to the test framework automatically and will cause the test to
	// fail, which also cancels the context passed to the group.
	Wait()
}

// ErrorGroup is an interface for managing a group of tasks, similar to Group,
// however ErrorGroup will not fail the test if a task returns an error
// (including panics), but will still cancel the context passed to the group.
// Errors are returned through the WaitE method.
type ErrorGroup interface {
	Tasker
	// WaitE waits for all tasks in the group to complete. Any errors from tasks
	// are returned and will cancel the context passed to the group. If the group
	// contains multiple subgroups, the errors from the groups will be combined
	// into a single error.
	WaitE() error
}

// GroupProvider is an interface for creating new Group(s). Generally, the test
// framework will supply a GroupProvider to tests.
type GroupProvider interface {
	// NewGroup creates a new Group to manage tasks. Any options passed to this
	// function will be applied to all tasks started by the group.
	NewGroup(opts ...Option) Group
	// NewErrorGroup creates a new ErrorGroup to manage tasks, similar to
	// NewGroup.
	NewErrorGroup(opts ...Option) ErrorGroup
}
