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
	// Wait waits for all tasks in the group to complete. Errors from tasks are reported to the
	// test framework automatically and will cause the test to fail, which also
	// cancels the context passed to the group.
	Wait()
}

// GroupProvider is an interface for creating new Group(s). Generally, the test
// framework will supply a GroupProvider to tests.
type GroupProvider interface {
	// NewGroup creates a new Group to manage tasks. Any options passed to this
	// function will be applied to all tasks started by the group.
	NewGroup(opts ...Option) Group
}
