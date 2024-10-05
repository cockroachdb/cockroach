// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package acprovider

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
)

// Provider is an interface through which the auto config runner
// can receive new tasks to run.
// For more details, see the package-level documentation for
// package "autoconfig".
type Provider interface {
	// EnvUpdate returns a channel that receives a message any time the
	// current set of environments change. The channel receives an
	// initial event immediately.
	EnvUpdate() <-chan struct{}

	// ActiveEnvironments returns the IDs of environments that
	// have tasks available for execution. A nil or empty array is
	// returned when there are no more tasks to run.
	ActiveEnvironments() []autoconfigpb.EnvironmentID

	// Peek will block waiting for the first task the provider believes
	// still needs to be run for the given environment. It will return
	// an error if the context is canceled while waiting. It will
	// return ErrNoMoreTasks to indicate the caller does not need
	// to process tasks for this environment any more.
	Peek(ctx context.Context, env autoconfigpb.EnvironmentID) (autoconfigpb.Task, error)

	// Pop will report the completion of all tasks with ID up to
	// completed for the given environment.
	Pop(ctx context.Context, env autoconfigpb.EnvironmentID, completed autoconfigpb.TaskID) error
}

type errNoMoreTasks struct{}

func (errNoMoreTasks) Error() string { return "no more tasks" }

// ErrNoMoreTasks can be checked with errors.Is on the result of Peek
// when an environment is not providing tasks for the foreseeable future.
var ErrNoMoreTasks error = errNoMoreTasks{}

// NoTaskProvider is a stub provider which delivers no tasks.
type NoTaskProvider struct{}

var _ Provider = NoTaskProvider{}

// EnvUpdate is part of the Provider interface.
func (NoTaskProvider) EnvUpdate() <-chan struct{} {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return ch
}

// ActiveEnvironments is part of the Provider interface.
func (NoTaskProvider) ActiveEnvironments() []autoconfigpb.EnvironmentID { return nil }

// Peek is part of the Provider interface.
func (NoTaskProvider) Peek(
	ctx context.Context, _ autoconfigpb.EnvironmentID,
) (autoconfigpb.Task, error) {
	return autoconfigpb.Task{}, ErrNoMoreTasks
}

// Pop is part of the Provider interface.
func (NoTaskProvider) Pop(context.Context, autoconfigpb.EnvironmentID, autoconfigpb.TaskID) error {
	return nil
}
