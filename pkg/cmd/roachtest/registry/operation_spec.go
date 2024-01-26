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
	// When Skip is set, this can contain more text to be printed in the logs
	// after the "--- SKIP" line.
	SkipDetails string

	Name string
	// Owner is the name of the team responsible for signing off on failures of
	// this test that happen in the release process. This must be one of a limited
	// set of values (the keys in the roachtestTeams map).
	Owner Owner
	// The maximum duration the test is allowed to run before it is considered
	// failed. If not specified, the default timeout is 10m before the test's
	// associated cluster expires. The timeout is always truncated to 10m before
	// the test's cluster expires.
	Timeout time.Duration

	// CompatibleClouds is the set of clouds this test can run on (e.g. AllClouds,
	// OnlyGCE, etc). Must be set.
	CompatibleClouds CloudSet

	// Suites is the set of suites this test is part of (e.g. Nightly, Weekly,
	// etc). Must be set, even if empty (see ManualOnly).
	Suites SuiteSet

	// Dependency specify the types of resources required for this roachtest
	// operation to work. This will be used to match this operation up with
	// eligible clusters to run.
	Dependency OperationDependency

	// CanRunConcurrently specifies whether multiple instances of this operation
	// can run in parallel.
	CanRunConcurrently bool

	// RequiresLicense indicates that the test requires an
	// enterprise license to run correctly. Use this to ensure
	// tests will fail-early if COCKROACH_DEV_LICENSE is not set
	// in the environment.
	RequiresLicense bool

	// Run is the operation function.
	Run func(ctx context.Context, o operation.Operation, c cluster.Cluster)

	// CleanupWaitTime is the min time to wait before running the Cleanup method.
	CleanupWaitTime time.Duration

	// Cleanup is the operation cleanup function, if defined.
	Cleanup func(ctx context.Context, o operation.Operation, c cluster.Cluster)
}
