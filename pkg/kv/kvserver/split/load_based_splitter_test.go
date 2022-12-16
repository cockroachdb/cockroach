// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package split

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/ycsb"
	"golang.org/x/exp/rand"
)

// This is a testing framework to benchmark load-based splitters, enabling
// experimenting and comparison of different designs and settings.
//
// This testing framework inputs:
// 1. Generator settings to generate the requests for the load splitter to
// record:
//   - Start key generator type (zipfian or uniform) and iMax
//   - Span length generator type (zipfian or uniform) and iMax
//   - Weight generator type (zipfian or uniform) and iMax.
// 2. Range request percent (percent of range requests [startKey, endKey) as
// opposed to point requests with just a start key).
// 3. Load-based splitter constructor.
// 4. Random seed.
//
// This testing framework performs the following work in runTest:
// 1. Calls generateRequests to generate the requests for the load splitter to
// record. The start key, span length, and weight of requests are generated
// using the input generator types and iMax, as well as the range request
// percent to determine if we have a point or range request.
// 2. Calls getOptimalKey to calculate the optimal split key assuming we are an
// oracle with access to all the request weighted keys, the total weight of
// request keys left of this key, and the total weight of request keys right of
// this key. The optimal key is found by sorting all the request keys and
// finding the request key that minimizes the difference between the total
// weight of the right side and the total weight of the left side.
// 3. Calls getKey to evaluate the split key found by the load-based splitter.
// This is done by constructing the load splitter, invoking the load splitter's
// Record function on all the generated requests, invoking the load splitter's
// Key function to get the split key, and calculating the total weight on the
// left and right sides. We also maintain the times it took to execute the load
// splitter's Record and Key functions.
//
// This testing framework also supports runTestRepeated, which repeats the test
// multiple times with different random numbers and calculating the average /
// max percentage differences of left / right weights and average execution
// times. This testing framework also supports runTestMultipleSettings, which
// runs tests with different settings and prints the test results for each
// setting.
//
// This testing framework is intended for benchmarking different load splitter
// approaches and settings in terms of quality of split key found and execution
// time, and will probably be in the code for at least a month after the time
// of writing and possibly longer if experimenting load splitter designs is
// ongoing or the testing framework is deemed useful in the future. If the load
// splitter's found split key becomes consistent, we may be able to assert on
// the split key quality of the load splitter.

const (
	zipfGenerator    = 0
	uniformGenerator = 1
	numIterations    = 200
)

type generator interface {
	Uint64() uint64
}

type config struct {
	lbs                 LoadBasedSplitter
	startKeyGenerator   generator
	spanLengthGenerator generator
	weightGenerator     generator
	rangeRequestPercent float64
	numRequests         int
	randSource          *rand.Rand
}

type request struct {
	span   roachpb.Span
	weight float64
}

type weightedKey struct {
	key    uint32
	weight float64
}

type lbsTestSettings struct {
	desc                    string
	startKeyGeneratorType   int
	startKeyGeneratorIMax   uint64
	spanLengthGeneratorType int
	spanLengthGeneratorIMax uint64
	weightGeneratorType     int
	weightGeneratorIMax     uint64
	rangeRequestPercent     float64
	numRequests             int
	lbs                     func(*rand.Rand) LoadBasedSplitter
	seed                    uint64
}

func uint32ToKey(key uint32) roachpb.Key {
	return keys.SystemSQLCodec.TablePrefix(key)
}

func generateRequests(config *config) ([]request, []weightedKey, float64) {
	var totalWeight float64
	requests := make([]request, 0, config.numRequests)
	weightedKeys := make([]weightedKey, 0, 2*config.numRequests)

	for i := 0; i < config.numRequests; i++ {
		startKey := uint32(config.startKeyGenerator.Uint64())
		spanLength := uint32(config.spanLengthGenerator.Uint64())
		weight := float64(config.weightGenerator.Uint64())

		var span roachpb.Span
		span.Key = uint32ToKey(startKey)
		if config.randSource.Float64() < config.rangeRequestPercent {
			endKey := startKey + spanLength
			span.EndKey = uint32ToKey(endKey)

			weightedKeys = append(weightedKeys, weightedKey{
				key:    startKey,
				weight: weight / 2,
			})
			weightedKeys = append(weightedKeys, weightedKey{
				key:    endKey,
				weight: weight / 2,
			})
		} else {
			weightedKeys = append(weightedKeys, weightedKey{
				key:    startKey,
				weight: weight,
			})
		}
		requests = append(requests, request{
			span:   span,
			weight: weight,
		})
		totalWeight += weight
	}
	return requests, weightedKeys, totalWeight
}

func getOptimalKey(
	weightedKeys []weightedKey, totalWeight float64,
) (optimalKey uint32, optimalLeftWeight, optimalRightWeight float64) {
	var optimalKeyPtr *uint32
	var leftWeight float64
	sort.Slice(weightedKeys, func(i, j int) bool {
		return weightedKeys[i].key < weightedKeys[j].key
	})
	for i := range weightedKeys {
		rightWeight := totalWeight - leftWeight
		// Find the split key that results in the smallest difference between the
		// total weight of keys on the right side and the total weight of keys on
		// the left side.
		if optimalKeyPtr == nil ||
			math.Abs(rightWeight-leftWeight) < math.Abs(optimalRightWeight-optimalLeftWeight) {
			optimalKeyPtr = &weightedKeys[i].key
			optimalLeftWeight = leftWeight
			optimalRightWeight = rightWeight
		}
		leftWeight += weightedKeys[i].weight
	}
	optimalKey = *optimalKeyPtr
	return
}

func getKey(
	config *config, requests []request, weightedKeys []weightedKey,
) (
	key uint32,
	leftWeight, rightWeight float64,
	recordExecutionTime, keyExecutionTime time.Duration,
) {
	recordStart := timeutil.Now()
	for _, request := range requests {
		config.lbs.Record(request.span, request.weight)
	}
	recordExecutionTime = timeutil.Since(recordStart)
	keyStart := timeutil.Now()
	keyRoachpbKey := config.lbs.Key()
	keyExecutionTime = timeutil.Since(keyStart)

	_, key, _ = keys.SystemSQLCodec.DecodeTablePrefix(keyRoachpbKey)
	for _, weightedKey := range weightedKeys {
		if weightedKey.key < key {
			leftWeight += weightedKey.weight
		} else {
			rightWeight += weightedKey.weight
		}
	}
	return
}

func runTest(
	config *config,
) (
	key, optimalKey uint32,
	leftWeight, rightWeight, optimalLeftWeight, optimalRightWeight float64,
	recordExecutionTime, keyExecutionTime time.Duration,
) {
	requests, weightedKeys, totalWeight := generateRequests(config)
	optimalKey, optimalLeftWeight, optimalRightWeight = getOptimalKey(weightedKeys, totalWeight)
	key, leftWeight, rightWeight, recordExecutionTime, keyExecutionTime = getKey(config, requests, weightedKeys)
	return
}

func newGenerator(randSource *rand.Rand, generatorType int, iMax uint64) generator {
	var g generator
	var err error
	if generatorType == zipfGenerator {
		g, err = ycsb.NewZipfGenerator(randSource, 1, iMax, 0.99, false)
	} else if generatorType == uniformGenerator {
		g, err = ycsb.NewUniformGenerator(randSource, 1, iMax)
	} else {
		panic("generatorType must be zipfGenerator or uniformGenerator")
	}
	if err != nil {
		panic(err)
	}
	return g
}

func runTestRepeated(
	settings *lbsTestSettings,
) (
	avgPercentDifference, maxPercentDifference, avgOptimalPercentDifference, maxOptimalPercentDifference float64,
	avgRecordExecutionTime, avgKeyExecutionTime time.Duration,
) {
	randSource := rand.New(rand.NewSource(settings.seed))
	startKeyGenerator := newGenerator(randSource, settings.startKeyGeneratorType, settings.startKeyGeneratorIMax)
	spanLengthGenerator := newGenerator(randSource, settings.spanLengthGeneratorType, settings.spanLengthGeneratorIMax)
	weightGenerator := newGenerator(randSource, settings.weightGeneratorType, settings.weightGeneratorIMax)
	for i := 0; i < numIterations; i++ {
		_, _, leftWeight, rightWeight, optimalLeftWeight, optimalRightWeight, recordExecutionTime, keyExecutionTime := runTest(&config{
			lbs:                 settings.lbs(randSource),
			startKeyGenerator:   startKeyGenerator,
			spanLengthGenerator: spanLengthGenerator,
			weightGenerator:     weightGenerator,
			rangeRequestPercent: settings.rangeRequestPercent,
			numRequests:         settings.numRequests,
			randSource:          randSource,
		})
		percentDifference := 100 * math.Abs(leftWeight-rightWeight) / (leftWeight + rightWeight)
		avgPercentDifference += percentDifference
		if maxPercentDifference < percentDifference {
			maxPercentDifference = percentDifference
		}
		optimalPercentDifference := 100 * math.Abs(optimalLeftWeight-optimalRightWeight) / (optimalLeftWeight + optimalRightWeight)
		avgOptimalPercentDifference += optimalPercentDifference
		if maxOptimalPercentDifference < optimalPercentDifference {
			maxOptimalPercentDifference = optimalPercentDifference
		}
		avgRecordExecutionTime += recordExecutionTime
		avgKeyExecutionTime += keyExecutionTime
	}
	avgRecordExecutionTime = time.Duration(avgRecordExecutionTime.Nanoseconds() / int64(numIterations))
	avgKeyExecutionTime = time.Duration(avgKeyExecutionTime.Nanoseconds() / int64(numIterations))
	avgPercentDifference /= numIterations
	avgOptimalPercentDifference /= numIterations
	return
}

func runTestMultipleSettings(settingsArr []lbsTestSettings) {
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 4, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w,
		"Description\t"+
			"Avg Percent Difference\t"+
			"Max Percent Difference\t"+
			"Avg Optimal Percent Difference\t"+
			"Max Optimal Percent Difference\t"+
			"Avg Record Execution Time\t"+
			"Avg Key Execution Time",
	)
	for _, settings := range settingsArr {
		avgPercentDifference, maxPercentDifference, avgOptimalPercentDifference, maxOptimalPercentDifference, avgRecordExecutionTime, avgKeyExecutionTime := runTestRepeated(&settings)
		_, _ = fmt.Fprintf(w, "%s\t%f\t%f\t%f\t%f\t%s\t%s\n",
			settings.desc,
			avgPercentDifference,
			maxPercentDifference,
			avgOptimalPercentDifference,
			maxOptimalPercentDifference,
			avgRecordExecutionTime,
			avgKeyExecutionTime)
	}
	_ = w.Flush()
	fmt.Print(buf.String())
}

func ExampleUnweightedFinder() {
	runTestMultipleSettings([]lbsTestSettings{
		{
			desc:                    "UnweightedFinder",
			startKeyGeneratorType:   zipfGenerator,
			startKeyGeneratorIMax:   10000000000,
			spanLengthGeneratorType: uniformGenerator,
			spanLengthGeneratorIMax: 1000,
			weightGeneratorType:     uniformGenerator,
			weightGeneratorIMax:     1,
			rangeRequestPercent:     0.95,
			numRequests:             13000,
			lbs: func(randSource *rand.Rand) LoadBasedSplitter {
				return NewUnweightedFinder(timeutil.Now(), randSource)
			},
			seed: 2022,
		},
	})
}

func ExampleWeightedFinder() {
	seed := uint64(2022)
	lbs := func(randSource *rand.Rand) LoadBasedSplitter {
		return NewWeightedFinder(timeutil.Now(), randSource)
	}
	runTestMultipleSettings([]lbsTestSettings{
		{
			desc:                    "WeightedFinder/startIMax=10000000000/spanIMax=1000",
			startKeyGeneratorType:   zipfGenerator,
			startKeyGeneratorIMax:   10000000000,
			spanLengthGeneratorType: uniformGenerator,
			spanLengthGeneratorIMax: 1000,
			weightGeneratorType:     uniformGenerator,
			weightGeneratorIMax:     10,
			rangeRequestPercent:     0.95,
			numRequests:             10000,
			lbs:                     lbs,
			seed:                    seed,
		},
		{
			desc:                    "WeightedFinder/startIMax=100000/spanIMax=1000",
			startKeyGeneratorType:   zipfGenerator,
			startKeyGeneratorIMax:   100000,
			spanLengthGeneratorType: uniformGenerator,
			spanLengthGeneratorIMax: 1000,
			weightGeneratorType:     uniformGenerator,
			weightGeneratorIMax:     10,
			rangeRequestPercent:     0.95,
			numRequests:             10000,
			lbs:                     lbs,
			seed:                    seed,
		},
		{
			desc:                    "WeightedFinder/startIMax=1000/spanIMax=100",
			startKeyGeneratorType:   zipfGenerator,
			startKeyGeneratorIMax:   1000,
			spanLengthGeneratorType: uniformGenerator,
			spanLengthGeneratorIMax: 100,
			weightGeneratorType:     uniformGenerator,
			weightGeneratorIMax:     10,
			rangeRequestPercent:     0.95,
			numRequests:             10000,
			lbs:                     lbs,
			seed:                    seed,
		},
		{
			desc:                    "WeightedFinder/startIMax=100000/spanIMax=1000/point",
			startKeyGeneratorType:   zipfGenerator,
			startKeyGeneratorIMax:   100000,
			spanLengthGeneratorType: uniformGenerator,
			spanLengthGeneratorIMax: 1000,
			weightGeneratorType:     uniformGenerator,
			weightGeneratorIMax:     10,
			rangeRequestPercent:     0,
			numRequests:             10000,
			lbs:                     lbs,
			seed:                    seed,
		},
		{
			desc:                    "WeightedFinder/startIMax=10000000000/spanIMax=1000/unweighted",
			startKeyGeneratorType:   zipfGenerator,
			startKeyGeneratorIMax:   10000000000,
			spanLengthGeneratorType: uniformGenerator,
			spanLengthGeneratorIMax: 1000,
			weightGeneratorType:     uniformGenerator,
			weightGeneratorIMax:     1,
			rangeRequestPercent:     0.95,
			numRequests:             10000,
			lbs:                     lbs,
			seed:                    seed,
		},
	})
}
