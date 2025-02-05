// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeeds

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
)

// RegisterChangefeeds registers several operations related to changefeeds in the registry.
func RegisterChangefeeds(r registry.Registry) {

	// Adds the operation for creating a changefeed job
	r.AddOperation(registry.OperationSpec{
		Name:               "create-changefeed-job",
		Owner:              registry.OwnerCDC,
		Timeout:            1 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runCreateChangefeeds,
	})
	// Adds the operation for canceling a changefeed job
	r.AddOperation(registry.OperationSpec{
		Name:               "cancel-changefeed-job",
		Owner:              registry.OwnerCDC,
		Timeout:            1 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runCancelChangefeeds,
	})

	// Adds the operation for pausing a changefeed job
	r.AddOperation(registry.OperationSpec{
		Name:               "pause-changefeed-job",
		Owner:              registry.OwnerCDC,
		Timeout:            1 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runPauseChangefeeds,
	})

	// Adds the operation for resuming a changefeed job
	r.AddOperation(registry.OperationSpec{
		Name:               "resume-changefeed-job",
		Owner:              registry.OwnerCDC,
		Timeout:            1 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runResumeChangefeeds,
	})
}
