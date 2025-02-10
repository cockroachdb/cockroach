// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package test

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

// DefaultCockroachPath is the path where the binary passed to the
// `--cockroach` flag will be made available in every node in the
// cluster.
const DefaultCockroachPath = "./cockroach"

// DefaultDeprecatedWorkloadPath is the path where the binary passed
// to the `--workload` flag will be made available in the workload
// node if one is provisioned.
const DefaultDeprecatedWorkloadPath = "./workload"

// Test is the interface through which roachtests interact with the
// test harness.
type Test interface {
	// StandardCockroach returns path to main cockroach binary, compiled
	// without runtime assertions.
	StandardCockroach() string
	// RuntimeAssertionsCockroach returns the path to cockroach
	// binary compiled with --crdb_test build tag, or an empty string if
	// no such binary was given.
	RuntimeAssertionsCockroach() string
	// Cockroach returns either StandardCockroach or RuntimeAssertionsCockroach,
	// picked randomly.
	Cockroach() string
	Name() string
	BuildVersion() *version.Version
	IsBuildVersion(string) bool // "vXX.YY"
	SnapshotPrefix() string
	Helper()
	// Spec returns the *registry.TestSpec as an interface{}.
	//
	// TODO(tbg): cleaning this up is mildly tricky. TestSpec has the Run field
	// which depends both on `test` (and `cluster`, though this matters less), so
	// we get cyclic imports. We should split up `Run` off of `TestSpec` and have
	// `TestSpec` live in `spec` to avoid this problem, but this requires a pass
	// through all registered roachtests to change how they register the test.
	Spec() interface{}
	VersionsBinaryOverride() map[string]string
	SkipInit() bool
	Skip(args ...interface{})
	Skipf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(string, ...interface{})
	FailNow()
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Failed() bool

	ArtifactsDir() string

	// PerfArtifactsDir is the directory on cluster nodes in which perf artifacts
	// reside. Upon success this directory is copied into test's ArtifactsDir from
	// each node in the cluster.
	PerfArtifactsDir() string

	// GoCoverArtifactsDir is the directory on cluster nodes in which coverage
	// profiles are dumped (or "" if go coverage is not enabled). At the end of
	// this test, this directory is copied into the test's ArtifactsDir from each
	// node in the cluster.
	GoCoverArtifactsDir() string

	L() *logger.Logger
	Progress(float64)
	Status(args ...interface{})
	AddParam(string, string)
	WorkerStatus(args ...interface{})
	WorkerProgress(float64)
	IsDebug() bool

	Go(task.Func, ...task.Option)
	GoWithCancel(task.Func, ...task.Option) context.CancelFunc
	NewGroup(...task.Option) task.Group
	NewErrorGroup(...task.Option) task.ErrorGroup

	// DeprecatedWorkload returns the path to the workload binary.
	// Don't use this, invoke `./cockroach workload` instead.
	DeprecatedWorkload() string

	// ExportOpenmetrics returns a boolean value that decides whether the
	// metrics should be exported in openmetrics format or JSON format.
	// If true, the stats exporter will export metrics in openmetrics format,
	// else, the exporter will export in the JSON format.
	ExportOpenmetrics() bool

	// GetRunId returns the run id of the roachtest run, this is set to build id
	// when ran from teamcity
	GetRunId() string

	// Owner returns the owner of the test
	Owner() string
}
