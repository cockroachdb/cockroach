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
	OperationRequiresZeroUnderreplicatedRanges
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
	//
	// TODO(bilal): Unused.
	CanRunConcurrently bool

	// Run is the operation function. It returns an OperationCleanup if this
	// operation requires additional cleanup steps afterwards (eg. dropping an
	// extra column that was created). A nil return value indicates no cleanup
	// necessary
	Run func(ctx context.Context, o operation.Operation, c cluster.Cluster) OperationCleanup
}
