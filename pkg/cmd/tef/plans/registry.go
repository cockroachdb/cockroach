// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package plans contains the plan registration entry point for the Task
// Execution Framework (TEF) in the CockroachDB repository.
//
// Plans that need to access CockroachDB internals (SQL, KV, cluster APIs) should
// be registered here. Plans that don't need CockroachDB dependencies can be
// registered in the private task-exec-framework repository instead.
//
// The TEF CLI (which lives in the private repo) will call this function to
// register all CockroachDB-specific plans.
package plans

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/tef/planners"
	"github.com/cockroachdb/cockroach/pkg/cmd/tef/plans/crdb_demo"
)

// RegisterPlans registers all TEF plan implementations from the CockroachDB repository.
// This function is called by the TEF CLI (in the private repo) during initialization.
//
// Plans that require CockroachDB dependencies should be registered here.
// Plans that don't need CockroachDB code can be registered in the private repo.
//
// Example:
//
//	func RegisterPlans(pr *planners.PlanRegistry) {
//	    pr.Register(&myplan.MyPlanRegistry{})
//	    pr.Register(&anotherplan.PlanRegistry{})
//	}
func RegisterPlans(pr *planners.PlanRegistry) {
	// Register the CockroachDB demo plan
	crdb_demo.RegisterCRDBDemoPlans(pr)

	// Add additional plan registrations here as needed.
	// Example: pr.Register(&myplan.MyPlanRegistry{})
}
