// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// IsMetamorphicBuild returns whether this build is metamorphic. By build being
// "metamorphic" we mean that some magic constants in the codebase might get
// initialized to non-default value.
// A build will become metamorphic with metamorphicBuildProbability probability
// if 'crdb_test' build flag is specified (this is the case for all test
// targets).
func IsMetamorphicBuild() bool {
	return metamorphicBuild
}

var metamorphicBuild bool

const (
	metamorphicBuildProbability = 0.8
	metamorphicValueProbability = 0.75
	metamorphicBoolProbability  = 0.5
)

// ConstantWithMetamorphicTestValue should be used to initialize "magic
// constants" that should be varied during test scenarios to check for bugs at
// boundary conditions. When metamorphicBuild is true, the test value will be
// used with metamorphicValueProbability probability. In all other cases, the
// production ("default") value will be used.
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
// var batchSize = util.ConstantWithMetamorphicTestValue("batch-size", 64, 1)
//
// This will often give your code a batch size of 1 in the crdb_test build
// configuration, increasing the amount of exercise the edge conditions get.
//
// The given name is used for logging.
func ConstantWithMetamorphicTestValue(name string, defaultValue, metamorphicValue int) int {
	if metamorphicBuild {
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

// DisableMetamorphicEnvVar can be used to disable metamorhpic tests for
// sub-processes. If it exists and is set to something truthy as defined by
// strconv.ParseBool then metamorphic testing will not be enabled.
const DisableMetamorphicEnvVar = "COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING"

func init() {
	if buildutil.CrdbTestBuild {
		disabled := envutil.EnvOrDefaultBool(DisableMetamorphicEnvVar, false)
		if !disabled {
			rng.r, _ = randutil.NewTestRand()
			metamorphicBuild = rng.r.Float64() < metamorphicBuildProbability
		}
	}
}

// ConstantWithMetamorphicTestRange is like ConstantWithMetamorphicTestValue
// except instead of returning a single metamorphic test value, it returns a
// random test value in the semi-open range [min, max).
//
// The given name is used for logging.
func ConstantWithMetamorphicTestRange(name string, defaultValue, min, max int) int {
	if metamorphicBuild {
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

// ConstantWithMetamorphicTestBool is like ConstantWithMetamorphicTestValue except
// it returns the non-default value half of the time (if running a metamorphic build).
//
// The given name is used for logging.
func ConstantWithMetamorphicTestBool(name string, defaultValue bool) bool {
	if metamorphicBuild {
		rng.Lock()
		defer rng.Unlock()
		if rng.r.Float64() < metamorphicBoolProbability {
			ret := !defaultValue
			logMetamorphicValue(name, ret)
			return ret
		}
	}
	return defaultValue
}

func logMetamorphicValue(name string, value interface{}) {
	fmt.Fprintf(os.Stderr, "initialized metamorphic constant %q with value %v\n", name, value)
}
