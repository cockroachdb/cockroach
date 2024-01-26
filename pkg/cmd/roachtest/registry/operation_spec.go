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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

// OperationDependency specifies what an operation requires
type OperationDependency int

const (
	OperationRequiresNodes OperationDependency = iota
	OperationRequiresSqlConnection
	OperationRequiresDatabaseSchema
	OperationRequiresPopulatedDatabase
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
	// failed.
	Timeout time.Duration

	// CompatibleClouds is the set of clouds this test can run on (e.g. AllClouds,
	// OnlyGCE, etc). Must be set.
	CompatibleClouds CloudSet

	// Dependency specify the types of resources required for this roachtest
	// operation to work. This will be used to match this operation up with
	// eligible clusters to run.
	//
	// TODO(bilal): Unused.
	Dependency OperationDependency

	// CanRunConcurrently specifies whether this operation is safe to run
	// concurrently with other operations that have CanRunConcurrently = true.
	// For instance, two random-index additions are safe to run concurrently,
	// while a drop would need to run on its own and will have
	// CanRunConcurrently = false.
	//
	// TODO(bilal): Unused.
	CanRunConcurrently bool

	// RequiresLicense indicates that the operation requires an
	// enterprise license to run correctly. Use this to ensure
	// operation will fail-early if COCKROACH_DEV_LICENSE is not set
	// in the environment.
	RequiresLicense bool

	// Run is the operation function.
	Run func(ctx context.Context, o operation.Operation, c cluster.Cluster)

	// CleanupWaitTime is the min time to wait before running the Cleanup method.
	CleanupWaitTime time.Duration

	// Cleanup is the operation cleanup function, if defined.
	Cleanup func(ctx context.Context, o operation.Operation, c cluster.Cluster)
}

// TestSpec() converts this operation to a TestSpec for use with roachtest run-operation.
func (s *OperationSpec) TestSpec() TestSpec {
	return TestSpec{
		Skip:              s.Skip,
		Name:              s.Name,
		Owner:             s.Owner,
		Timeout:           s.Timeout,
		Benchmark:         false,
		Cluster:           spec.ClusterSpec{NodeCount: 1},
		CompatibleClouds:  AllClouds,
		NonReleaseBlocker: true,
		Operation:         true,
		RequiresLicense:   s.RequiresLicense,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			wrapper := &operationWrapper{Test: t, cleanupState: make(map[string]string)}
			s.Run(ctx, wrapper, c)

			if s.Cleanup != nil {
				t.Status(fmt.Sprintf("operation ran successfully; waiting %s before cleanup", s.CleanupWaitTime.String()))
				if s.CleanupWaitTime != 0 {
					select {
					case <-ctx.Done():
						t.Status("bailing due to cancellation")
						return
					case <-time.After(s.CleanupWaitTime):
					}
				}

				s.Cleanup(ctx, wrapper, c)
			}
		},
		CockroachBinary: StandardCockroach,
	}
}

// operationWrapper turns a test.Test into an operation.Operation.
type operationWrapper struct {
	test.Test
	cleanupState map[string]string
}

func (o *operationWrapper) SetCleanupState(key, value string) {
	o.cleanupState[key] = value
}

func (o *operationWrapper) GetCleanupState(key string) string {
	return o.cleanupState[key]
}
