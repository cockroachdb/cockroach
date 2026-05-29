// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package registry

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
)

// OperationDependency specifies what an operation requires from a cluster to
// be able to run. The zero value is the simplest dependency (i.e. an existant
// cluster with nodes) and is always implied even if unspecified. All non-zero
// values could require additional pre-Run checks from any runner/scheduler
// to verify if this operation can be run.
type OperationDependency int

const (
	OperationRequiresNodes OperationDependency = iota
	OperationRequiresPopulatedDatabase
	OperationRequiresZeroUnavailableRanges
	OperationRequiresZeroUnderreplicatedRanges
	OperationRequiresLDRJobRunning
	OperationRequiresRunningBackupJob
	OperationRequiresRunningRestoreJob
)

// OperationIsolation specifies to what extent the operation runner will try
// to isolate this operation runner from other operations.
type OperationIsolation int

const (
	// OperationCanRunConcurrently denotes operations that can run concurrently
	// with themselves as well as with other operations.
	OperationCanRunConcurrently OperationIsolation = iota
	// OperationCannotRunConcurrentlyWithItself denotes operations that cannot run
	// concurrently with other iterations of itself, but can run concurrently with
	// other operations.
	OperationCannotRunConcurrentlyWithItself
	// OperationCannotRunConcurrently denotes operations that cannot run concurrently
	// in any capacity, and lock out all other operations while they run.
	OperationCannotRunConcurrently
)

// OperationCleanup specifies an operation that
type OperationCleanup interface {
	Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster)
}

// OperationSpec is a spec for a roachtest operation.
type OperationSpec struct {
	Skip string // if non-empty, operation will be skipped

	Name string
	// Owner is the name of the team responsible for signing off on failures of
	// this operation that happen in the release process. This must be one of a limited
	// set of values (the keys in the roachtestTeams map).
	Owner Owner
	// The maximum duration the operation is allowed to run before it is considered
	// failed. This timeout applies _individually_ to Run() and the returned
	// OperationCleanup Cleanup() each, but does not include any scheduling waits
	// for either method.
	Timeout time.Duration

	// CompatibleClouds is the set of clouds this operation can run on (e.g. AllClouds,
	// OnlyGCE, etc). Must be set.
	CompatibleClouds CloudSet

	// Dependencies specify the types of resources required for this roachtest
	// operation to work. This will be used in filtering eligible operations to
	// run. Multiple dependencies could be specified, and any schedulers will take
	// care of ensuring those dependencies are met before running Run().
	Dependencies []OperationDependency

	// CanRunConcurrently specifies whether this operation is safe to run
	// concurrently with other operations that have CanRunConcurrently = true. For
	// instance, a random-index addition is safe to run concurrently with most
	// other operations like node kills, while a drop would need to run on its own
	// and will have CanRunConcurrently = false.
	CanRunConcurrently OperationIsolation

	// WaitBeforeCleanup specifies the amount of time to wait before running
	// the cleanup function. This overrides the default wait time set by the flag
	// --wait-before-cleanup.
	WaitBeforeCleanup time.Duration

	// DeferCleanup, when true, causes the worker to enqueue cleanup for
	// later execution and immediately become available for the next
	// operation. The cleanup executes after WaitBeforeCleanup elapses.
	// Only set this for non-destructive operations whose Run phase does
	// not degrade cluster state during the wait.
	DeferCleanup bool

	// LongRunning marks this operation as long-running. Long-running
	// operations execute in a dedicated goroutine, separate from the normal
	// worker pool, and run sequentially (one at a time). Their Run()
	// functions are expected to take a long time (e.g., consistency checks,
	// range probes). Long-running operations must be non-destructive to
	// cluster state — never kill nodes, network-partition.
	LongRunning bool

	// Cadence overrides the global --wait-before-next-execution interval for
	// this specific operation. When zero, the global default applies.
	// Available for all operations (both normal and long-running).
	Cadence time.Duration

	// Run is the operation function. It returns an OperationCleanup if this
	// operation requires additional cleanup steps afterwards (eg. dropping an
	// extra column that was created). A nil return value indicates no cleanup
	// necessary
	Run func(ctx context.Context, o operation.Operation, c cluster.Cluster) OperationCleanup
}

// NamePrefix returns the first part of `o.Name` after splitting with delimiter `/`.
func (o *OperationSpec) NamePrefix() string {
	parts := strings.Split(o.Name, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return o.Name
}

// DedupKey returns the key used to deduplicate concurrent operation runs.
// Short-lived operations use the name prefix so that variants like
// "drain/node-1" and "drain/node-2" are mutually exclusive. Long-running
// operations use the full name so each variant can run independently.
func (o *OperationSpec) DedupKey() string {
	if o.LongRunning {
		return o.Name
	}
	return o.NamePrefix()
}
