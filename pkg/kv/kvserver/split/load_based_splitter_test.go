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
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/ycsb"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

const (
	zipfGenerator    = 0
	uniformGenerator = 1
)

type LoadBasedSplitter interface {
	Record(span roachpb.Span, weight float32)
	Key() roachpb.Key
}

type Generator interface {
	Uint64() uint64
}

type Config struct {
	lbs                 LoadBasedSplitter
	startKeyGenerator   Generator
	spanLengthGenerator Generator
	weightGenerator     Generator
	rangeRequestPercent float64
	numRequests         int
	randSource          *rand.Rand
}

type Request struct {
	span   roachpb.Span
	weight float32
}

type WeightedKey struct {
	key    uint32
	weight float32
}

func uint32ToKey(key uint32) roachpb.Key {
	return keys.SystemSQLCodec.TablePrefix(key)
}

func runTest(
	config *Config,
) (
	key, optimalKey uint32,
	leftWeight, rightWeight, optimalLeftWeight, optimalRightWeight float32,
	recordExecutionTime, keyExecutionTime time.Duration,
) {
	var totalWeight float32
	requests := make([]Request, 0, config.numRequests)
	weightedKeys := make([]WeightedKey, 0, 2*config.numRequests)

	for i := 0; i < config.numRequests; i++ {
		startKey := uint32(config.startKeyGenerator.Uint64())
		spanLength := uint32(config.spanLengthGenerator.Uint64())
		weight := float32(config.weightGenerator.Uint64())

		var span roachpb.Span
		span.Key = uint32ToKey(startKey)
		if config.randSource.Float64() < config.rangeRequestPercent {
			endKey := startKey + spanLength
			span.EndKey = uint32ToKey(endKey)

			weightedKeys = append(weightedKeys, WeightedKey{
				key:    startKey,
				weight: weight / 2,
			})
			weightedKeys = append(weightedKeys, WeightedKey{
				key:    endKey,
				weight: weight / 2,
			})
		} else {
			weightedKeys = append(weightedKeys, WeightedKey{
				key:    startKey,
				weight: weight,
			})
		}
		requests = append(requests, Request{
			span:   span,
			weight: weight,
		})
		totalWeight += weight
	}

	sort.Slice(requests, func(i, j int) bool {
		return weightedKeys[i].key < weightedKeys[j].key
	})
	var optimalKeyPtr *uint32
	var prefixTotalWeight float32
	for _, weightedKey := range weightedKeys {
		if optimalKeyPtr == nil || math.Abs(float64(totalWeight-2*prefixTotalWeight)) < math.Abs(float64(totalWeight-2*optimalLeftWeight)) {
			optimalKeyPtr = &weightedKey.key
			optimalLeftWeight = prefixTotalWeight
		}
		prefixTotalWeight += weightedKey.weight
	}
	optimalKey = *optimalKeyPtr
	optimalRightWeight = totalWeight - optimalLeftWeight

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

type Settings struct {
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

func newGenerator(t *testing.T, randSource *rand.Rand, generatorType int, iMax uint64) Generator {
	var generator Generator
	var err error
	if generatorType == zipfGenerator {
		generator, err = ycsb.NewZipfGenerator(randSource, 1, iMax, 0.99, false)
	} else if generatorType == uniformGenerator {
		generator, err = ycsb.NewUniformGenerator(randSource, 1, iMax)
	} else {
		require.Error(t, nil, "generatorType must be zipfGenerator or uniformGenerator")
	}
	require.NoError(t, err)
	return generator
}

func runTestRepeated(
	t *testing.T, settings *Settings,
) (
	avgPercentDifference, maxPercentDifference, avgOptimalPercentDifference, maxOptimalPercentDifference float32,
	avgRecordExecutionTime, avgKeyExecutionTime time.Duration,
) {
	numIterations := 10

	randSource := rand.New(rand.NewSource(settings.seed))
	startKeyGenerator := newGenerator(t, randSource, settings.startKeyGeneratorType, settings.startKeyGeneratorIMax)
	spanLengthGenerator := newGenerator(t, randSource, settings.spanLengthGeneratorType, settings.spanLengthGeneratorIMax)
	weightGenerator := newGenerator(t, randSource, settings.weightGeneratorType, settings.weightGeneratorIMax)
	for i := 0; i < numIterations; i++ {
		_, _, leftWeight, rightWeight, optimalLeftWeight, optimalRightWeight, recordExecutionTime, keyExecutionTime := runTest(&Config{
			lbs:                 settings.lbs(randSource),
			startKeyGenerator:   startKeyGenerator,
			spanLengthGenerator: spanLengthGenerator,
			weightGenerator:     weightGenerator,
			rangeRequestPercent: settings.rangeRequestPercent,
			numRequests:         settings.numRequests,
			randSource:          randSource,
		})
		percentDifference := float32(100 * math.Abs(float64(leftWeight-rightWeight)) / math.Abs(float64(leftWeight+rightWeight)))
		avgPercentDifference += percentDifference
		if maxPercentDifference < percentDifference {
			maxPercentDifference = percentDifference
		}
		optimalPercentDifference := float32(100 * math.Abs(float64(optimalLeftWeight-optimalRightWeight)) / math.Abs(float64(optimalLeftWeight+optimalRightWeight)))
		avgOptimalPercentDifference += optimalPercentDifference
		if maxOptimalPercentDifference < optimalPercentDifference {
			maxOptimalPercentDifference = optimalPercentDifference
		}
		avgRecordExecutionTime += recordExecutionTime
		avgKeyExecutionTime += keyExecutionTime
	}
	avgRecordExecutionTime = time.Duration(avgRecordExecutionTime.Nanoseconds() / int64(numIterations))
	avgKeyExecutionTime = time.Duration(avgKeyExecutionTime.Nanoseconds() / int64(numIterations))
	avgPercentDifference /= float32(numIterations)
	avgOptimalPercentDifference /= float32(numIterations)
	return
}

func runTestMultipleSettings(t *testing.T, settingsArr []Settings) {
	fmt.Printf(
		"%30s%26s%26s%34s%34s%29s%26s\n",
		"Description",
		"Avg Percent Difference",
		"Max Percent Difference",
		"Avg Optimal Percent Difference",
		"Max Optimal Percent Difference",
		"Avg Record Execution Time",
		"Avg Key Execution Time",
	)
	for _, settings := range settingsArr {
		avgPercentDifference, maxPercentDifference, avgOptimalPercentDifference, maxOptimalPercentDifference, avgRecordExecutionTime, avgKeyExecutionTime := runTestRepeated(t, &settings)
		fmt.Printf(
			"%30s%26f%26f%34f%34f%29s%26s\n",
			settings.desc,
			avgPercentDifference,
			maxPercentDifference,
			avgOptimalPercentDifference,
			maxOptimalPercentDifference,
			avgRecordExecutionTime,
			avgKeyExecutionTime,
		)
	}
}

func TestUnweightedFinder(t *testing.T) {
	runTestMultipleSettings(t, []Settings{
		{
			desc:                    "Unweighted Finder",
			startKeyGeneratorType:   zipfGenerator,
			startKeyGeneratorIMax:   10000000000,
			spanLengthGeneratorType: uniformGenerator,
			spanLengthGeneratorIMax: 1000,
			weightGeneratorType:     uniformGenerator,
			weightGeneratorIMax:     1,
			rangeRequestPercent:     0.95,
			numRequests:             10000,
			lbs: func(randSource *rand.Rand) LoadBasedSplitter {
				return NewTestFinder(randSource)
			},
			seed: 2022,
		},
	})
}
