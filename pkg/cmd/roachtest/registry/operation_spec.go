// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package registry

import (
	"context"
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
	OperationRequiresZeroLaggingRanges
)

// OperationSpec is a spec for a roachtest operation.
type OperationSpec struct {
	Skip string // if non-empty, operation will be skipped

	Name string
	// Owner is the name of the team responsible for signing off on failures of
	// this operation that happen in the release process. This must be one of a limited
	// set of values (the keys in the roachtestTeams map).
	Owner Owner
	// The maximum duration the operation is allowed to run before it is considered
	// failed. This timeout applies _individually_ to Run() and Cleanup() each, but
	// does not include any scheduling waits for either method.
	Timeout time.Duration

	// CompatibleClouds is the set of clouds this operation can run on (e.g. AllClouds,
	// OnlyGCE, etc). Must be set.
	CompatibleClouds CloudSet

	// Dependency specify the types of resources required for this roachtest
	// operation to work. This will be used in filtering eligible operations to
	// run. Multiple dependencies could be specified, and any schedulers will take
	// care of ensuring those dependencies are met before running Run().
	//
	// TODO(bilal): Unused.
	Dependency []OperationDependency

	// CanRunConcurrently specifies whether this operation is safe to run
	// concurrently with other operations that have CanRunConcurrently = true. For
	// instance, a random-index addition is safe to run concurrently with most
	// other operations like node kills, while a drop would need to run on its own
	// and will have CanRunConcurrently = false.
	//
	// TODO(bilal): Unused.
	CanRunConcurrently bool

	// Run is the operation function.
	Run func(ctx context.Context, o operation.Operation, c cluster.Cluster)

	// CleanupWaitTime is the min time to wait before running the Cleanup method.
	// Note that schedulers/runners are free to wait longer than this amount of
	// time. Any intermediary wait of this sort does not count towards Timeout.
	CleanupWaitTime time.Duration

	// Cleanup is the operation cleanup function, if defined.
	Cleanup func(ctx context.Context, o operation.Operation, c cluster.Cluster)
}
