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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

const (
	// Zero and One are the possible return values for a BinaryChoice.
	Zero = iota
	One
)

// BinaryChoice allows the selection of two possible options; the
// random number generator and the probability of a choice are
// customizable using `BinaryChoiceOption`.
type BinaryChoice struct {
	rng             *rand.Rand
	probabilityZero float64
}

type BinaryChoiceOption func(*BinaryChoice)

// RNGOption allows callers to use their own random number generator
// when making a binary choice.
func RNGOption(rng *rand.Rand) BinaryChoiceOption {
	return func(bc *BinaryChoice) {
		bc.rng = rng
	}
}

// ProbabilityOption allows callers to set their own probability of
// returning `0` when generating a binary choice.
func ProbabilityOption(p float64) BinaryChoiceOption {
	return func(bc *BinaryChoice) {
		bc.probabilityZero = p
	}
}

// NewBinaryChoice generates a BinaryChoice with the options provided,
// if any.
func NewBinaryChoice(opts ...BinaryChoiceOption) *BinaryChoice {
	var result BinaryChoice
	for _, opt := range opts {
		opt(&result)
	}

	return &result
}

// Generate returns either Zero or One, based on the value produced by
// the underlying random number generator and the probability of a
// Zero choice.
func (bc *BinaryChoice) Generate() int {
	var val float64
	if bc.rng == nil {
		val = rand.Float64()
	} else {
		val = bc.rng.Float64()
	}

	if val < bc.probabilityZero {
		return Zero
	}

	return One
}

// Test is the interface through which roachtests interact with the
// test harness.
type Test interface {
	// StandardCockroach returns path to main cockroach binary, compiled
	// without runtime assertions.
	StandardCockroach() string
	// RuntimeAssertionsCockroach returns the path to cockroach-short
	// binary compiled with --crdb_test build tag, or an empty string if
	// no such binary was given.
	RuntimeAssertionsCockroach() string
	// Cockroach returns either StandardCockroach or RuntimeAssertionsCockroach,
	// picked randomly based on the options passed.
	Cockroach(...BinaryChoiceOption) string
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
	L() *logger.Logger
	Progress(float64)
	Status(args ...interface{})
	WorkerStatus(args ...interface{})
	WorkerProgress(float64)
	IsDebug() bool

	// DeprecatedWorkload returns the path to the workload binary.
	// Don't use this, invoke `./cockroach workload` instead.
	DeprecatedWorkload() string
}
