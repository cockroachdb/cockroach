// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metamorphic

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic/metamorphicutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// IsMetamorphicBuild returns whether this build is metamorphic. By build being
// "metamorphic" we mean that some magic constants in the codebase might get
// initialized to non-default value.
// A build will become metamorphic with metamorphicutil.IsMetamorphicBuildProbability probability
// if 'crdb_test' build flag is specified (this is the case for all test
// targets).
func IsMetamorphicBuild() bool {
	return metamorphicutil.IsMetamorphicBuild
}

const (
	IsMetamorphicBuildProbability = 0.8
	metamorphicValueProbability   = 0.75
	metamorphicBoolProbability    = 0.5
)

// ConstantWithTestValue should be used to initialize "magic constants" that
// should be varied during test scenarios to check for bugs at boundary
// conditions. When metamorphicutil.IsMetamorphicBuild is true, the test value will be used with
// metamorphicValueProbability probability. In all other cases, the production
// ("default") value will be used.
// The constant must be a "metamorphic variable": changing it cannot affect the
// output of any SQL DMLs. It can only affect the way in which the data is
// retrieved or processed, because otherwise the main test corpus would fail if
// this flag were enabled.
//
// An example of a "magic constant" that behaves this way is a batch size. Batch
// sizes tend to present testing problems, because often the logic that deals
// with what to do when a batch is finished is less likely to be exercised by
// simple unit tests that don't use enough data to fill up a batch.
//
// For example, instead of writing:
//
// const batchSize = 64
//
// you should write:
//
// var batchSize = metamorphic.ConstantWithTestValue("batch-size", 64, 1)
//
// This will often give your code a batch size of 1 in the crdb_test build
// configuration, increasing the amount of exercise the edge conditions get.
//
// The given name is used for logging.
func ConstantWithTestValue(name string, defaultValue, metamorphicValue int) int {
	if metamorphicutil.IsMetamorphicBuild {
		rng.Lock()
		defer rng.Unlock()
		if rng.r.Float64() < metamorphicValueProbability {
			logMetamorphicValue(name, metamorphicValue)
			return metamorphicValue
		}
	}
	return defaultValue
}

// rng is initialized to a rand.Rand if crdbTestBuild is enabled.
var rng struct {
	r *rand.Rand
	syncutil.Mutex
}

// DisableMetamorphicEnvVar can be used to disable metamorphic tests for
// sub-processes. If it exists and is set to something truthy as defined by
// strconv.ParseBool then metamorphic testing will not be enabled.
const DisableMetamorphicEnvVar = "COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING"

// Returns true iff the current process is eligible to enable metamorphic
// variables. When run under Bazel, checking if we are in the Go test wrapper
// ensures that metamorphic variables are not initialized and logged twice
// from both the wrapper and the main test process, as both will perform
// initialization of the test module and its dependencies.
func metamorphicEligible() bool {
	if !buildutil.CrdbTestBuild {
		return false
	}

	if bazel.InTestWrapper() {
		return false
	}

	return true
}

func init() {
	if metamorphicEligible() {
		if !disableMetamorphicTesting {
			rng.r, _ = randutil.NewTestRand()
			metamorphicutil.IsMetamorphicBuild = rng.r.Float64() < IsMetamorphicBuildProbability
		}
	}
}

// ConstantWithTestRange is like ConstantWithTestValue except instead of
// returning a single metamorphic test value, it returns a random test value in
// the semi-open range [min, max).
//
// The given name is used for logging.
func ConstantWithTestRange(name string, defaultValue, min, max int) int {
	if metamorphicutil.IsMetamorphicBuild {
		rng.Lock()
		defer rng.Unlock()
		if rng.r.Float64() < metamorphicValueProbability {
			ret := min
			if max > min {
				ret = int(rng.r.Int31())%(max-min) + min
			}
			logMetamorphicValue(name, ret)
			return ret
		}
	}
	return defaultValue
}

// ConstantWithTestBool is like ConstantWithTestValue except it returns the
// non-default value half of the time (if running a metamorphic build).
//
// The given name is used for logging.
func ConstantWithTestBool(name string, defaultValue bool) bool {
	return constantWithTestBoolInternal(name, defaultValue, true /* doLog */)
}

func constantWithTestBoolInternal(name string, defaultValue bool, doLog bool) bool {
	if metamorphicutil.IsMetamorphicBuild {
		rng.Lock()
		defer rng.Unlock()
		if rng.r.Float64() < metamorphicBoolProbability {
			ret := !defaultValue
			if doLog {
				logMetamorphicValue(name, ret)
			}
			return ret
		}
	}
	return defaultValue
}

// ConstantWithTestBoolWithoutLogging is like ConstantWithTestBool except it
// does not log the value. This is necessary to work around this issue:
// https://github.com/cockroachdb/cockroach/issues/106667
// TODO(test-eng): Remove this variant when the issue above is addressed.
func ConstantWithTestBoolWithoutLogging(name string, defaultValue bool) bool {
	return constantWithTestBoolInternal(name, defaultValue, false /* doLog */)
}

// ConstantWithTestChoice is like ConstantWithTestValue except it returns a
// random choice (equally weighted) of the given values. The default value is
// included in the random choice.
//
// The given name is used for logging.
func ConstantWithTestChoice(
	name string, defaultValue interface{}, otherValues ...interface{},
) interface{} {
	if metamorphicutil.IsMetamorphicBuild {
		values := append([]interface{}{defaultValue}, otherValues...)
		rng.Lock()
		defer rng.Unlock()
		value := values[rng.r.Int63n(int64(len(values)))]
		logMetamorphicValue(name, value)
		return value
	}
	return defaultValue
}

func logMetamorphicValue(name string, value interface{}) {
	fmt.Fprintf(os.Stderr, "initialized metamorphic constant %q with value %v\n", name, value)
}
