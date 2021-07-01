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
	Spec() interface{} // main.TestSpec, TODO(tbg): clean up
	VersionsBinaryOverride() map[string]string
	Skip(args ...interface{})
	Skipf(format string, args ...interface{})
	Errorf(string, ...interface{})
	FailNow()
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Failed() bool
	ArtifactsDir() string
	L() *logger.Logger
	Progress(float64)
	Status(args ...interface{})
	WorkerStatus(args ...interface{})
	WorkerProgress(float64)
}
