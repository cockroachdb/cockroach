// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package split

import (
	"bytes"
	"context"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
)

type DFLargestRandSource struct{}

func (r DFLargestRandSource) Float64() float64 {
	return 0
}

// Intn returns the largest number possible in [0, n)
func (r DFLargestRandSource) Intn(n int) int {
	var result int
	if n > 0 {
		result = n - 1
	}
	return result
}

// TestSplitFinderKey verifies the Key() method correctly
// finds an appropriate split point for the range.
func TestSplitFinderKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	const ReservoirKeyOffset = 1000

	// Test an empty reservoir (reservoir without load).
	basicReservoir := [splitKeySampleSize]sample{}

	// Test reservoir with no load should have no splits.
	noLoadReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      0,
			right:     0,
			contained: 0,
		}
		noLoadReservoir[i] = tempSample
	}

	// Test a uniform reservoir.
	uniformReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      splitKeyMinCounter,
			right:     splitKeyMinCounter,
			contained: 0,
		}
		uniformReservoir[i] = tempSample
	}

	// Testing a non-uniform reservoir.
	nonUniformReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      splitKeyMinCounter * i,
			right:     splitKeyMinCounter * (splitKeySampleSize - i),
			contained: 0,
		}
		nonUniformReservoir[i] = tempSample
	}

	// Test a load heavy reservoir on a single hot key (the last key).
	singleHotKeyReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      0,
			right:     splitKeyMinCounter,
			contained: 0,
		}
		singleHotKeyReservoir[i] = tempSample
	}

	// Test a load heavy reservoir on multiple hot keys (first and last key).
	multipleHotKeysReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      splitKeyMinCounter,
			right:     splitKeyMinCounter,
			contained: 0,
		}
		multipleHotKeysReservoir[i] = tempSample
	}
	multipleHotKeysReservoir[0].left = 0

	// Test a spanning reservoir where splits shouldn't occur.
	spanningReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      0,
			right:     0,
			contained: splitKeyMinCounter,
		}
		spanningReservoir[i] = tempSample
	}

	// Test that splits happen between two heavy spans.
	multipleSpanReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      splitKeyMinCounter,
			right:     splitKeyMinCounter,
			contained: splitKeyMinCounter,
		}
		multipleSpanReservoir[i] = tempSample
	}
	midSample := sample{
		key:       keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + splitKeySampleSize/2)),
		left:      splitKeyMinCounter,
		right:     splitKeyMinCounter,
		contained: 0,
	}
	multipleSpanReservoir[splitKeySampleSize/2] = midSample

	testCases := []struct {
		reservoir      [splitKeySampleSize]sample
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
		{multipleSpanReservoir, keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + splitKeySampleSize/2)},
	}

	randSource := rand.New(rand.NewSource(2022))
	for i, test := range testCases {
		finder := NewUnweightedFinder(timeutil.Now(), randSource)
		finder.samples = test.reservoir
		if splitByLoadKey := finder.Key(); !bytes.Equal(splitByLoadKey, test.splitByLoadKey) {
			t.Errorf(
				"%d: expected splitByLoadKey: %v, but got splitByLoadKey: %v",
				i, test.splitByLoadKey, splitByLoadKey)
		}
	}
}

// TestSplitFinderRecorder verifies the Record() method correctly
// records a span.
func TestSplitFinderRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	const ReservoirKeyOffset = 1000

	// Test recording a key query before the reservoir is full.
	basicReservoir := [splitKeySampleSize]sample{}
	basicSpan := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset),
		EndKey: keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + 1),
	}
	expectedBasicReservoir := [splitKeySampleSize]sample{}
	expectedBasicReservoir[0] = sample{
		key: basicSpan.Key,
	}

	// Test recording a key query after the reservoir is full with replacement.
	replacementReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      0,
			right:     0,
			contained: 0,
		}
		replacementReservoir[i] = tempSample
	}
	replacementSpan := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + splitKeySampleSize),
		EndKey: keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + splitKeySampleSize + 1),
	}
	expectedReplacementReservoir := replacementReservoir
	expectedReplacementReservoir[0] = sample{
		key: replacementSpan.Key,
	}

	// Test recording a key query after the reservoir is full without replacement.
	fullReservoir := replacementReservoir
	fullSpan := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset),
		EndKey: keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + 1),
	}
	expectedFullReservoir := fullReservoir
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.SystemSQLCodec.TablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      1,
			right:     0,
			contained: 0,
		}
		expectedFullReservoir[i] = tempSample
	}
	expectedFullReservoir[0].left = 0
	expectedFullReservoir[0].right = 1

	// Test recording a spanning query.
	spanningReservoir := replacementReservoir
	spanningSpan := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset - 1),
		EndKey: keys.SystemSQLCodec.TablePrefix(ReservoirKeyOffset + splitKeySampleSize + 1),
	}
	expectedSpanningReservoir := spanningReservoir
	for i := 0; i < splitKeySampleSize; i++ {
		expectedSpanningReservoir[i].contained++
	}

	testCases := []struct {
		recordSpan        roachpb.Span
		randSource        RandSource
		currCount         int
		currReservoir     [splitKeySampleSize]sample
		expectedReservoir [splitKeySampleSize]sample
	}{
		// Test recording a key query before the reservoir is full.
		{basicSpan, DFLargestRandSource{}, 0, basicReservoir, expectedBasicReservoir},
		// Test recording a key query after the reservoir is full with replacement.
		{replacementSpan, ZeroRandSource{}, splitKeySampleSize + 1, replacementReservoir, expectedReplacementReservoir},
		// Test recording a key query after the reservoir is full without replacement.
		{fullSpan, DFLargestRandSource{}, splitKeySampleSize + 1, fullReservoir, expectedFullReservoir},
		// Test recording a spanning query.
		{spanningSpan, DFLargestRandSource{}, splitKeySampleSize + 1, spanningReservoir, expectedSpanningReservoir},
	}

	for i, test := range testCases {
		finder := NewUnweightedFinder(timeutil.Now(), test.randSource)
		finder.samples = test.currReservoir
		finder.count = test.currCount
		finder.Record(test.recordSpan, 1)
		if !reflect.DeepEqual(finder.samples, test.expectedReservoir) {
			t.Errorf(
				"%d: expected reservoir: %v, but got reservoir: %v",
				i, test.expectedReservoir, finder.samples)
		}
	}
}

func TestFinderNoSplitKeyCause(t *testing.T) {
	samples := [splitKeySampleSize]sample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		if i < 5 {
			// Insufficient counters.
			samples[idx] = sample{
				key:       keys.SystemSQLCodec.TablePrefix(uint32(i)),
				left:      0,
				right:     0,
				contained: splitKeyMinCounter - 1,
			}
		} else if i < 7 {
			// Imbalance and too many contained counters.
			deviationLeft := rand.Intn(5)
			deviationRight := rand.Intn(5)
			samples[idx] = sample{
				key:       keys.SystemSQLCodec.TablePrefix(uint32(i)),
				left:      25 + deviationLeft,
				right:     15 - deviationRight,
				contained: int(max(float64(splitKeyMinCounter-40-deviationLeft+deviationRight), float64(40+deviationLeft-deviationRight))),
			}
		} else if i < 13 {
			// Imbalance counters.
			deviationLeft := rand.Intn(5)
			deviationRight := rand.Intn(5)
			samples[idx] = sample{
				key:       keys.SystemSQLCodec.TablePrefix(uint32(i)),
				left:      50 + deviationLeft,
				right:     30 - deviationRight,
				contained: int(max(float64(splitKeyMinCounter-80-deviationLeft+deviationRight), 0)),
			}
		} else {
			// Too many contained counters.
			contained := int(splitKeyMinCounter*splitKeyContainedThreshold + 1)
			left := (splitKeyMinCounter - contained) / 2
			samples[idx] = sample{
				key:       keys.SystemSQLCodec.TablePrefix(uint32(i)),
				left:      left,
				right:     splitKeyMinCounter - left - contained,
				contained: contained,
			}
		}
	}

	randSource := rand.New(rand.NewSource(2022))
	finder := NewUnweightedFinder(timeutil.Now(), randSource)
	finder.samples = samples
	insufficientCounters, imbalance, tooManyContained, imbalanceAndTooManyContained := finder.noSplitKeyCause()
	assert.Equal(t, 5, insufficientCounters, "unexpected insufficient counters")
	assert.Equal(t, 6, imbalance, "unexpected imbalance counters")
	assert.Equal(t, 7, tooManyContained, "unexpected too many contained counters")
	assert.Equal(t, 2, imbalanceAndTooManyContained, "unexpected imbalance and too many contained counters")
}

func TestFinderPopularKeyFrequency(t *testing.T) {
	uniqueKeySample := [splitKeySampleSize]sample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		uniqueKeySample[idx] = sample{
			key: keys.SystemSQLCodec.TablePrefix(uint32(i)),
		}
	}
	twentyPercentPopularKeySample := [splitKeySampleSize]sample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		var tableID uint32
		if i <= 15 {
			tableID = uint32(i / 3)
		} else {
			tableID = 6
		}
		twentyPercentPopularKeySample[idx] = sample{
			key: keys.SystemSQLCodec.TablePrefix(tableID),
		}
	}
	twentyFivePercentPopularKeySample := [splitKeySampleSize]sample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		var tableID uint32
		if i < 8 || i >= 13 {
			tableID = uint32(i / 4)
		} else {
			tableID = 2
		}
		twentyFivePercentPopularKeySample[idx] = sample{
			key: keys.SystemSQLCodec.TablePrefix(tableID),
		}
	}
	fiftyPercentPopularKeySample := [splitKeySampleSize]sample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		fiftyPercentPopularKeySample[idx] = sample{
			key: keys.SystemSQLCodec.TablePrefix(uint32(i / 10)),
		}
	}
	fiftyFivePercentPopularKeySample := [splitKeySampleSize]sample{}
	for i, idx := range rand.Perm(splitKeySampleSize) {
		var tableID uint32
		if i >= 11 {
			tableID = uint32(1)
		}
		fiftyFivePercentPopularKeySample[idx] = sample{
			key: keys.SystemSQLCodec.TablePrefix(tableID),
		}
	}
	sameKeySample := [splitKeySampleSize]sample{}
	for _, idx := range rand.Perm(splitKeySampleSize) {
		sameKeySample[idx] = sample{
			key: keys.SystemSQLCodec.TablePrefix(0),
		}
	}

	testCases := []struct {
		samples                     [splitKeySampleSize]sample
		expectedPopularKeyFrequency float64
	}{
		{uniqueKeySample, 0.05},
		{twentyPercentPopularKeySample, 0.2},
		{twentyFivePercentPopularKeySample, 0.25},
		{fiftyPercentPopularKeySample, 0.5},
		{fiftyFivePercentPopularKeySample, 0.55},
		{sameKeySample, 1},
	}

	randSource := rand.New(rand.NewSource(2022))
	for i, test := range testCases {
		finder := NewUnweightedFinder(timeutil.Now(), randSource)
		finder.samples = test.samples
		popularKeyFrequency := finder.PopularKeyFrequency()
		assert.Equal(t, test.expectedPopularKeyFrequency, popularKeyFrequency, "unexpected popular key frequency in test %d", i)
	}
}
