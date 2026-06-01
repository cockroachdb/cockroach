// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import "github.com/cockroachdb/errors"

// Registry maps hook type names to their executor implementations.
type Registry struct {
	executors map[string]HookExecutor
}

// NewRegistry creates an empty hook registry.
func NewRegistry() *Registry {
	return &Registry{executors: make(map[string]HookExecutor)}
}

// Register adds an executor for the given hook type name.
func (r *Registry) Register(hookType string, executor HookExecutor) {
	r.executors[hookType] = executor
}

// Get returns the executor for the given hook type name. Returns an error if
// no executor is registered for that type.
func (r *Registry) Get(hookType string) (HookExecutor, error) {
	executor, ok := r.executors[hookType]
	if !ok {
		return nil, errors.Newf("no executor registered for hook type %q", hookType)
	}
	return executor, nil
}
