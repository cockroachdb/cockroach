// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeeds

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/jobs"
)

// runCreateChangefeeds creates a new changefeed job if the number of current changefeeds is below the allowed maximum.
func runCreateChangefeeds(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	// Establish a connection to the cluster.
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() {
		_ = conn.Close()
	}()

	err := checkAndCreateChangefeed(ctx, o, conn, false)
	if err != nil {
		fmt.Printf("%v", err)
		o.Fatal(err)
	}
	return nil // No cleanup operation needed.
}

// createRun creates the run for the action and monitors based on the currentStates and expectedFinalStates
func createRun(
	action string, currentStates, expectedFinalStates []jobs.State,
) func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		err := changeStateOrCreateChangefeed(ctx, o, c, action, currentStates, expectedFinalStates)
		if err != nil {
			o.Fatal(err)
		}

		// Return a nil cleanup function as there's no specific cleanup required here.
		return nil
	}
}

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
		Run:                createRun("CANCEL", []jobs.State{jobs.StateRunning, jobs.StatePaused}, []jobs.State{jobs.StateCanceled}),
	})

	// Adds the operation for pausing a changefeed job
	r.AddOperation(registry.OperationSpec{
		Name:               "pause-changefeed-job",
		Owner:              registry.OwnerCDC,
		Timeout:            1 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                createRun("PAUSE", []jobs.State{jobs.StateRunning}, []jobs.State{jobs.StatePaused}),
	})

	// Adds the operation for resuming a changefeed job
	r.AddOperation(registry.OperationSpec{
		Name:               "resume-changefeed-job",
		Owner:              registry.OwnerCDC,
		Timeout:            1 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                createRun("RESUME", []jobs.State{jobs.StatePaused}, []jobs.State{jobs.StateRunning}),
	})
}
