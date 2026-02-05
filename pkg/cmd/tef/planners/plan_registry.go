// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide the plan registry management for the Task
// Execution Framework (TEF).
package planners

// PlanRegistry manages a collection of Registry instances, providing
// centralized registration and retrieval of plan registries.
type PlanRegistry struct {
	registries []Registry
}

// NewPlanRegistry creates and returns a new PlanRegistry with an empty
// collection of registries.
func NewPlanRegistry() *PlanRegistry {
	return &PlanRegistry{
		registries: make([]Registry, 0),
	}
}

// Register adds one or more Registry instances to the PlanRegistry.
// Multiple registries can be registered in a single call.
func (pr *PlanRegistry) Register(registry ...Registry) {
	pr.registries = append(pr.registries, registry...)
}

// GetRegistries returns all registered Registry instances.
func (pr *PlanRegistry) GetRegistries() []Registry {
	return pr.registries
}
