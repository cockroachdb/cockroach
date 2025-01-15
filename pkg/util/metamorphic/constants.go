// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metamorphic

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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
	return getConstantInternal(name,
		defaultValue,
		strconv.Atoi,
		metamorphicValueProbability,
		func(r *rand.Rand) int {
			return metamorphicValue
		},
		true)
}

// ConstantWithTestRange is like ConstantWithTestValue except instead of
// returning a single metamorphic test value, it returns a random test value in
// the semi-open range [min, max).
//
// The given name is used for logging.
func ConstantWithTestRange(name string, defaultValue, min, max int) int {
	return getConstantInternal(name,
		defaultValue,
		strconv.Atoi,
		metamorphicValueProbability,
		func(r *rand.Rand) int {
			ret := min
			if max > min {
				ret = int(r.Int31())%(max-min) + min
			}
			return ret
		},
		true)
}

// ConstantWithTestBool is like ConstantWithTestValue except it returns the
// non-default value half of the time (if running a metamorphic build).
//
// The given name is used for logging.
func ConstantWithTestBool(name string, defaultValue bool) bool {
	return constantWithTestBoolInternal(name, defaultValue, true /* doLog */)
}

// ConstantWithTestBoolWithoutLogging is like ConstantWithTestBool except it
// does not log the value. This is necessary to work around this issue:
// https://github.com/cockroachdb/cockroach/issues/106667
// TODO(test-eng): Remove this variant when the issue above is addressed.
func ConstantWithTestBoolWithoutLogging(name string, defaultValue bool) bool {
	return constantWithTestBoolInternal(name, defaultValue, false /* doLog */)
}

func constantWithTestBoolInternal(name string, defaultValue bool, doLog bool) bool {
	return getConstantInternal(name,
		defaultValue,
		strconv.ParseBool,
		metamorphicBoolProbability,
		func(*rand.Rand) bool { return !defaultValue },
		doLog)
}

// ConstantWithTestChoice is like ConstantWithTestValue except it returns a
// random choice (equally weighted) of the given values. The default value is
// included in the random choice.
//
// The given name is used for logging.
func ConstantWithTestChoice[T any](name string, defaultValue T, otherValues ...T) T {
	return getConstantInternal(name,
		defaultValue,
		func(s string) (T, error) {
			v, err := parseChoice[T](s)
			return v.(T), err
		},
		1.0,
		func(r *rand.Rand) T {
			values := append([]T{defaultValue}, otherValues...)
			return values[rng.r.Int63n(int64(len(values)))]
		},
		true)
}

// getConstantInternal returns a value of type T for the given name. If the name
// has been specified as an override, the parseValue func is used to parse the
// user-provided value. If no such override is specified, it either returns the
// defaultValue or a value generated by generateValue()
func getConstantInternal[T any](
	name string,
	defaultValue T,
	parseValue func(string) (T, error),
	probability float64,
	generateValue func(r *rand.Rand) T,
	doLog bool,
) T {
	overrideVal, hasOverride := valueFromOverride(name, parseValue)
	// This is structured so that we make the same number uses of
	// `rng.r` in the case of a metamoprhic build so that the seed
	// value is more useful.
	if metamorphicutil.IsMetamorphicBuild || hasOverride {
		rng.Lock()
		defer rng.Unlock()
		shouldGenerate := rng.r.Float64() < probability
		if shouldGenerate || hasOverride {
			ret := generateValue(rng.r)
			src := randomVal
			if hasOverride {
				src = override
				ret = overrideVal
			}

			if doLog {
				logMetamorphicValue(name, ret, src)
			}
			return ret
		}
	}
	return defaultValue
}

// parseChoice tries to parse the given string into the type T. The returned
// value should be safe to cast to type T.
func parseChoice[T any](s string) (any, error) {
	var zero T
	switch any(zero).(type) {
	case string:
		return s, nil
	case int, int16, int32, int64:
		return strconv.Atoi(s)
	case float32:
		return strconv.ParseFloat(s, 32)
	case float64:
		return strconv.ParseFloat(s, 64)
	default:
		panic(fmt.Sprintf("unable to parse %T", zero))
	}
}

type metamorphicSource string

const (
	randomVal metamorphicSource = ""
	override  metamorphicSource = " (from override)"
)

func logMetamorphicValue(name string, value interface{}, src metamorphicSource) {
	logf("initialized metamorphic constant %q with value %v%s\n", name, value, src)
}

func logf(fmtStr string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, fmtStr, args...)
}

const (
	// DisableMetamorphicEnvVar can be used to disable metamorphic tests for
	// sub-processes. If it exists and is set to something truthy as defined by
	// strconv.ParseBool then metamorphic testing will not be enabled.
	DisableMetamorphicEnvVar = "COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING"
	// MetamorphicOverridesEnvVar can be used to set metamorphic
	// variables to specific values.
	MetamorphicOverridesEnvVar = "COCKROACH_INTERNAL_METAMORPHIC_OVERRIDES"
)

var (
	// overrides holds user-provided values parsed from
	// MetamorphicOverridesEnvVar.
	overrides = map[string]string{}

	// rng is initialized to a rand.Rand if crdbTestBuild is enabled.
	rng struct {
		r *rand.Rand
		syncutil.Mutex
	}
	// Exposed for testing.
	rngSeed int64
)

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

// valueFromOverride retuns any user-provided override value for the given name.
// The passed parser function is used to parse the value from a string
// representation.
func valueFromOverride[T any](name string, parser func(string) (T, error)) (T, bool) {
	if v, ok := overrides[name]; ok {
		ret, err := parser(v)
		if err != nil {
			panic(fmt.Sprintf("malformed value for %s: %s: %s", name, v, err.Error()))
		}
		return ret, true
	} else {
		var zero T
		return zero, false
	}
}

func init() {
	if metamorphicEligible() {
		if !disableMetamorphicTesting {
			rng.r, rngSeed = randutil.NewPseudoRandWithGlobalSeed()
			metamorphicutil.IsMetamorphicBuild = rng.r.Float64() < IsMetamorphicBuildProbability
			if metamorphicutil.IsMetamorphicBuild {
				logf("metamorphic: use COCKROACH_RANDOM_SEED=%d for reproduction\n", rngSeed)
			} else {
				logf("IsMetamorphicBuild==false\n")
			}
		}

		if overrideList, ok := envutil.EnvString(MetamorphicOverridesEnvVar, 0); ok {
			logf("metamorphic: using overrides from environment\n")
			setOverridesFromString(overrideList)
		}
	}
}

func setOverridesFromString(overrideList string) {
	overrideItems := strings.Split(overrideList, ",")
	for _, item := range overrideItems {
		itemParts := strings.SplitN(item, "=", 2)
		if len(itemParts) < 2 {
			panic(fmt.Sprintf("malformed override: %s", item))
		}
		overrides[itemParts[0]] = itemParts[1]
	}
}
