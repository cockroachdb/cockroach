// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package test

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

// Test is the interface through which roachtests interact with the
// test harness.
type Test interface {
	Cockroach() string // path to main cockroach binary
	Name() string
	BuildVersion() *version.Version
	IsBuildVersion(string) bool // "vXX.YY"
	Helper()
	// Spec() returns the *registry.TestSpec as an interface{}.
	//
	// TODO(tbg): cleaning this up is mildly tricky. TestSpec has the Run field
	// which depends both on `test` (and `cluster`, though this matters less), so
	// we get cyclic imports. We should split up `Run` off of `TestSpec` and have
	// `TestSpec` live in `spec` to avoid this problem, but this requires a pass
	// through all registered roachtests to change how they register the test.
	Spec() interface{}
	VersionsBinaryOverride() map[string]string
	Skip(args ...interface{})
	Skipf(format string, args ...interface{})
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
	L() *logger.Logger
	Progress(float64)
	Status(args ...interface{})
	WorkerStatus(args ...interface{})
	WorkerProgress(float64)

	// DeprecatedWorkload returns the path to the workload binary.
	// Don't use this, invoke `./cockroach workload` instead.
	DeprecatedWorkload() string
}
