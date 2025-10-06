// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package split

import (
	"bytes"
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

type ZeroRandSource struct{}

func (r ZeroRandSource) Float64() float64 {
	return 0
}

func (r ZeroRandSource) Intn(int) int {
	return 0
}

type WFLargestRandSource struct{}

func (r WFLargestRandSource) Float64() float64 {
	return 1
}

func (r WFLargestRandSource) Intn(int) int {
	return 0
}

// TestSplitWeightedFinderKey verifies the Key() method correctly
// finds an appropriate split point for the range.
func TestSplitWeightedFinderKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	const ReservoirKeyOffset = 1000

	// Test an empty reservoir (reservoir without load).
	basicReservoir := [splitKeySampleSize]weightedSample{}

	// Test reservoir with no load should have no splits.
	noLoadReservoir := [splitKeySampleSize]weightedSample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := weightedSample{
			key:   keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:  0,
			right: 0,
			count: 0,
		}
		noLoadReservoir[i] = tempSample
	}

	// Test a uniform reservoir.
	uniformReservoir := [splitKeySampleSize]weightedSample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := weightedSample{
			key:   keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:  splitKeyMinCounter,
			right: splitKeyMinCounter,
			count: splitKeyMinCounter,
		}
		uniformReservoir[i] = tempSample
	}

	// Testing a non-uniform reservoir.
	nonUniformReservoir := [splitKeySampleSize]weightedSample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := weightedSample{
			key:   keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:  float64(splitKeyMinCounter * i),
			right: float64(splitKeyMinCounter * (splitKeySampleSize - i)),
			count: splitKeyMinCounter,
		}
		nonUniformReservoir[i] = tempSample
	}

	// Test a load heavy reservoir on a single hot key (the last key).
	singleHotKeyReservoir := [splitKeySampleSize]weightedSample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := weightedSample{
			key:   keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:  0,
			right: splitKeyMinCounter,
			count: splitKeyMinCounter,
		}
		singleHotKeyReservoir[i] = tempSample
	}

	// Test a load heavy reservoir on multiple hot keys (first and last key).
	multipleHotKeysReservoir := [splitKeySampleSize]weightedSample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := weightedSample{
			key:   keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:  splitKeyMinCounter,
			right: splitKeyMinCounter,
			count: splitKeyMinCounter,
		}
		multipleHotKeysReservoir[i] = tempSample
	}
	multipleHotKeysReservoir[0].left = 0

	// Test a spanning reservoir where splits shouldn't occur.
	spanningReservoir := [splitKeySampleSize]weightedSample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := weightedSample{
			key:   keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:  75,
			right: 45,
			count: splitKeyMinCounter,
		}
		spanningReservoir[i] = tempSample
	}

	// Test that splits happen in best balance score.
	bestBalanceReservoir := [splitKeySampleSize]weightedSample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := weightedSample{
			key:   keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:  2.3 * splitKeyMinCounter,
			right: 1.7 * splitKeyMinCounter,
			count: splitKeyMinCounter,
		}
		bestBalanceReservoir[i] = tempSample
	}
	midSample := weightedSample{
		key:   keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + splitKeySampleSize/2)),
		left:  1.1 * splitKeyMinCounter,
		right: 0.9 * splitKeyMinCounter,
		count: splitKeyMinCounter,
	}
	bestBalanceReservoir[splitKeySampleSize/2] = midSample

	testCases := []struct {
		reservoir      [splitKeySampleSize]weightedSample
		splitByLoadKey roachpb.Key
	}{
		// Test an empty reservoir.
		{basicReservoir, nil},
		// Test reservoir with no load should have no splits.
		{noLoadReservoir, nil},
		// Test a uniform reservoir (Splits at the first key)
		{uniformReservoir, keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset)},
		// Testing a non-uniform reservoir.
		{nonUniformReservoir, keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + splitKeySampleSize/2)},
		// Test a load heavy reservoir on a single hot key. Splitting can't help here.
		{singleHotKeyReservoir, nil},
		// Test a load heavy reservoir on multiple hot keys. Splits between the hot keys.
		{multipleHotKeysReservoir, keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + 1)},
		// Test a spanning reservoir. Splitting will be bad here. Should avoid it.
		{spanningReservoir, nil},
		// Test that splits happen between two heavy spans.
		{bestBalanceReservoir, keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + splitKeySampleSize/2)},
	}

	randSource := rand.New(rand.NewSource(2022))
	for i, test := range testCases {
		weightedFinder := NewWeightedFinder(timeutil.Now(), randSource)
		weightedFinder.samples = test.reservoir
		if splitByLoadKey := weightedFinder.Key(); !bytes.Equal(splitByLoadKey, test.splitByLoadKey) {
			t.Errorf(
				"%d: expected splitByLoadKey: %v, but got splitByLoadKey: %v",
				i, test.splitByLoadKey, splitByLoadKey)
		}
	}
}

// TestSplitWeightedFinderRecorder verifies the Record() method correctly
// records a span.
func TestSplitWeightedFinderRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	const ReservoirKeyOffset = 1000

	// Test recording a key query before the reservoir is full.
	basicReservoir := [splitKeySampleSize]weightedSample{}
	basicSpan := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset),
		EndKey: keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + 1),
	}
	const basicWeight = 1
	expectedBasicReservoir := [splitKeySampleSize]weightedSample{}
	expectedBasicReservoir[0] = weightedSample{
		key:    basicSpan.Key,
		weight: 0.5,
	}
	expectedBasicReservoir[1] = weightedSample{
		key:    basicSpan.EndKey,
		weight: 0.5,
	}

	// Test recording a key query after the reservoir is full with replacement.
	replacementReservoir := [splitKeySampleSize]weightedSample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := weightedSample{
			key:    keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			weight: 1,
			left:   0,
			right:  0,
		}
		replacementReservoir[i] = tempSample
	}
	replacementSpan := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + splitKeySampleSize),
		EndKey: keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + splitKeySampleSize + 1),
	}
	const replacementWeight = 1
	expectedReplacementReservoir := replacementReservoir
	expectedReplacementReservoir[0] = weightedSample{
		key:    replacementSpan.EndKey,
		weight: 0.5,
	}

	// Test recording a key query after the reservoir is full without replacement.
	fullReservoir := replacementReservoir
	fullSpan := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset),
		EndKey: keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + 1),
	}
	const fullWeight = 1
	expectedFullReservoir := fullReservoir
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := weightedSample{
			key:    keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			weight: 1,
			left:   1,
			right:  0,
			count:  2,
		}
		expectedFullReservoir[i] = tempSample
	}
	expectedFullReservoir[0].left = 0
	expectedFullReservoir[0].right = 1
	expectedFullReservoir[1].left = 0.5
	expectedFullReservoir[1].right = 0.5

	// Test recording a spanning query.
	spanningReservoir := replacementReservoir
	spanningSpan := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset - 1),
		EndKey: keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + splitKeySampleSize + 1),
	}
	const spanningWeight = 1
	expectedSpanningReservoir := spanningReservoir
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := weightedSample{
			key:    keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			weight: 1,
			left:   0.5,
			right:  0.5,
			count:  2,
		}
		expectedSpanningReservoir[i] = tempSample
	}

	testCases := []struct {
		recordSpan        roachpb.Span
		weight            float64
		randSource        RandSource
		currCount         int
		currReservoir     [splitKeySampleSize]weightedSample
		expectedReservoir [splitKeySampleSize]weightedSample
	}{
		// Test recording a key query before the reservoir is full.
		{basicSpan, basicWeight, WFLargestRandSource{}, 0, basicReservoir, expectedBasicReservoir},
		// Test recording a key query after the reservoir is full with replacement.
		{replacementSpan, replacementWeight, ZeroRandSource{}, splitKeySampleSize + 1, replacementReservoir, expectedReplacementReservoir},
		// Test recording a key query after the reservoir is full without replacement.
		{fullSpan, fullWeight, WFLargestRandSource{}, splitKeySampleSize + 1, fullReservoir, expectedFullReservoir},
		// Test recording a spanning query.
		{spanningSpan, spanningWeight, WFLargestRandSource{}, splitKeySampleSize + 1, spanningReservoir, expectedSpanningReservoir},
	}

	for i, test := range testCases {
		weightedFinder := NewWeightedFinder(timeutil.Now(), test.randSource)
		weightedFinder.samples = test.currReservoir
		weightedFinder.count = test.currCount
		weightedFinder.totalWeight = 100
		weightedFinder.Record(test.recordSpan, test.weight)
		if !reflect.DeepEqual(weightedFinder.samples, test.expectedReservoir) {
			t.Errorf(
				"%d: expected reservoir: %v, but got reservoir: %v",
				i, test.expectedReservoir, weightedFinder.samples)
		}
	}
}

func TestWeightedFinderNoSplitKeyCause(t *testing.T) {
	samples := [splitKeySampleSize]weightedSample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		if i < 7 {
			// Insufficient counters.
			samples[idx] = weightedSample{
				key:    keys.SystemSQLCodec.TablePrefix(uint32(i)),
				weight: 1,
				left:   splitKeyMinCounter,
				right:  splitKeyMinCounter,
				count:  splitKeyMinCounter - 1,
			}
		} else {
			// Imbalance counters.
			deviationLeft := 5 * rand.Float64()
			deviationRight := 5 * rand.Float64()
			samples[idx] = weightedSample{
				key:    keys.SystemSQLCodec.TablePrefix(uint32(i)),
				weight: 1,
				left:   75 + deviationLeft,
				right:  45 - deviationRight,
				count:  splitKeyMinCounter,
			}
		}
	}

	randSource := rand.New(rand.NewSource(2022))
	weightedFinder := NewWeightedFinder(timeutil.Now(), randSource)
	weightedFinder.samples = samples
	insufficientCounters, imbalance := weightedFinder.noSplitKeyCause()
	assert.Equal(t, 7, insufficientCounters, "unexpected insufficient counters")
	assert.Equal(t, 13, imbalance, "unexpected imbalance counters")
}

func TestWeightedFinderPopularKeyFrequency(t *testing.T) {
	uniqueKeyUnweightedSample := [splitKeySampleSize]weightedSample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		uniqueKeyUnweightedSample[idx] = weightedSample{
			key:    keys.SystemSQLCodec.TablePrefix(uint32(i)),
			weight: 1,
		}
	}
	uniqueKeyWeightedSample := [splitKeySampleSize]weightedSample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		uniqueKeyWeightedSample[idx] = weightedSample{
			key:    keys.SystemSQLCodec.TablePrefix(uint32(i)),
			weight: float64(i + 1),
		}
	}
	duplicateKeyUnweightedSample := [splitKeySampleSize]weightedSample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		var tableID uint32
		if i < 8 || i >= 13 {
			tableID = uint32(i / 4)
		} else {
			tableID = 2
		}
		duplicateKeyUnweightedSample[idx] = weightedSample{
			key:    keys.SystemSQLCodec.TablePrefix(tableID),
			weight: 1,
		}
	}
	duplicateKeyWeightedSample := [splitKeySampleSize]weightedSample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		var tableID uint32
		if i < 8 || i >= 15 {
			tableID = uint32(i / 4)
		} else {
			tableID = 2
		}
		duplicateKeyWeightedSample[idx] = weightedSample{
			key:    keys.SystemSQLCodec.TablePrefix(tableID),
			weight: float64(i + 1),
		}
	}
	sameKeySample := [splitKeySampleSize]weightedSample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		sameKeySample[idx] = weightedSample{
			key:    keys.SystemSQLCodec.TablePrefix(0),
			weight: float64(i),
		}
	}

	const eps = 1e-3
	testCases := []struct {
		samples                     [splitKeySampleSize]weightedSample
		expectedPopularKeyFrequency float64
	}{
		{uniqueKeyUnweightedSample, 1.0 / 20.0},
		{uniqueKeyWeightedSample, 20.0 / 210.0}, // 20/(1+2+...+20)
		{duplicateKeyUnweightedSample, 5.0 / 20.0},
		{duplicateKeyWeightedSample, 84.0 / 210.0}, // (9+10+...+15)/(1+2+...+20)
		{sameKeySample, 1},
	}

	randSource := rand.New(rand.NewSource(2022))
	for i, test := range testCases {
		weightedFinder := NewWeightedFinder(timeutil.Now(), randSource)
		weightedFinder.samples = test.samples
		popularKeyFrequency := weightedFinder.PopularKeyFrequency()
		assert.True(t, math.Abs(test.expectedPopularKeyFrequency-popularKeyFrequency) < eps,
			"%d: expected popular key frequency %f, got %f",
			i, test.expectedPopularKeyFrequency, popularKeyFrequency)
	}
}
