// Copyright 2019 The Cockroach Authors.
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
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecider(t *testing.T) {
	defer leaktest.AfterTest(t)()

	intn := rand.New(rand.NewSource(12)).Intn

	var d Decider
	Init(&d, intn, func() float64 { return 10.0 })

	ms := func(i int) time.Time {
		ts, err := time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
		assert.NoError(t, err)
		return ts.Add(time.Duration(i) * time.Millisecond)
	}

	op := func(s string) func() roachpb.Span {
		return func() roachpb.Span { return roachpb.Span{Key: roachpb.Key(s)} }
	}

	assertQPS := func(i int, expQPS float64) {
		t.Helper()
		qps := d.LastQPS(ms(i))
		assert.Equal(t, expQPS, qps)
	}

	assert.Equal(t, false, d.Record(ms(100), 1, nil))
	assertQPS(100, 0)

	// The first operation was interpreted as having happened after an eternity
	// of no activity, and rolled over the qps to mark the beginning of a new
	// second. The next qps computation is expected for timestamps >= 1100.
	assert.Equal(t, ms(100), d.mu.lastQPSRollover)
	assert.EqualValues(t, 0, d.mu.count)

	assert.Equal(t, false, d.Record(ms(400), 4, nil))
	assertQPS(100, 0)
	assertQPS(700, 0)

	assert.Equal(t, false, d.Record(ms(300), 3, nil))
	assertQPS(100, 0)

	assert.Equal(t, false, d.Record(ms(900), 1, nil))
	assertQPS(0, 0)

	assert.Equal(t, false, d.Record(ms(1099), 1, nil))
	assertQPS(0, 0)

	// Now 9 operations happened in the interval [100, 1099]. The next higher
	// timestamp will decide whether to engage the split finder.

	// It won't engage because the duration between the rollovers is 1.1s, and
	// we had 10 events over that interval.
	assert.Equal(t, false, d.Record(ms(1200), 1, nil))
	assertQPS(0, float64(10)/float64(1.1))
	assert.Equal(t, ms(1200), d.mu.lastQPSRollover)

	var nilFinder *Finder

	assert.Equal(t, nilFinder, d.mu.splitFinder)

	assert.Equal(t, false, d.Record(ms(2199), 12, nil))
	assert.Equal(t, nilFinder, d.mu.splitFinder)

	// 2200 is the next rollover point, and 12+1=13 qps should be computed.
	assert.Equal(t, false, d.Record(ms(2200), 1, op("a")))
	assert.Equal(t, ms(2200), d.mu.lastQPSRollover)
	assertQPS(0, float64(13))

	assert.NotNil(t, d.mu.splitFinder)
	assert.False(t, d.mu.splitFinder.Ready(ms(10)))

	// With continued partitioned write load, split finder eventually tells us
	// to split. We don't test the details of exactly when that happens because
	// this is done in the finder tests.
	tick := 2200
	for o := op("a"); !d.Record(ms(tick), 11, o); tick += 1000 {
		if tick/1000%2 == 0 {
			o = op("z")
		} else {
			o = op("a")
		}
	}

	assert.Equal(t, roachpb.Key("z"), d.MaybeSplitKey(ms(tick)))

	// We were told to split, but won't be told to split again for some time
	// to avoid busy-looping on split attempts.
	for i := 0; i <= int(minSplitSuggestionInterval/time.Second); i++ {
		o := op("z")
		if i%2 != 0 {
			o = op("a")
		}
		assert.False(t, d.Record(ms(tick), 11, o))
		assert.True(t, d.LastQPS(ms(tick)) > 1.0)
		// Even though the split key remains.
		assert.Equal(t, roachpb.Key("z"), d.MaybeSplitKey(ms(tick+999)))
		tick += 1000
	}
	// But after minSplitSuggestionInterval of ticks, we get another one.
	assert.True(t, d.Record(ms(tick), 11, op("a")))
	assert.True(t, d.LastQPS(ms(tick)) > 1.0)

	// Split key suggestion vanishes once qps drops.
	tick += 1000
	assert.False(t, d.Record(ms(tick), 9, op("a")))
	assert.Equal(t, roachpb.Key(nil), d.MaybeSplitKey(ms(tick)))
	assert.Equal(t, nilFinder, d.mu.splitFinder)

	// Hammer a key with writes above threshold. There shouldn't be a split
	// since everyone is hitting the same key and load can't be balanced.
	for i := 0; i < 1000; i++ {
		assert.False(t, d.Record(ms(tick), 11, op("q")))
		tick += 1000
	}
	assert.True(t, d.mu.splitFinder.Ready(ms(tick)))
	assert.Equal(t, roachpb.Key(nil), d.MaybeSplitKey(ms(tick)))

	// But the finder keeps sampling to adapt to changing workload...
	for i := 0; i < 1000; i++ {
		assert.False(t, d.Record(ms(tick), 11, op("p")))
		tick += 1000
	}

	// ... which we verify by looking at its samples directly.
	for _, sample := range d.mu.splitFinder.samples {
		assert.Equal(t, roachpb.Key("p"), sample.key)
	}

	// Since the new workload is also not partitionable, nothing changes in
	// the decision.
	assert.True(t, d.mu.splitFinder.Ready(ms(tick)))
	assert.Equal(t, roachpb.Key(nil), d.MaybeSplitKey(ms(tick)))

	// Get the decider engaged again so that we can test Reset().
	for i := 0; i < 1000; i++ {
		o := op("z")
		if i%2 != 0 {
			o = op("a")
		}
		d.Record(ms(tick), 11, o)
		tick += 500
	}

	// The finder wants to split, until Reset is called, at which point it starts
	// back up at zero.
	assert.True(t, d.mu.splitFinder.Ready(ms(tick)))
	assert.Equal(t, roachpb.Key("z"), d.MaybeSplitKey(ms(tick)))
	d.Reset()
	assert.Nil(t, d.MaybeSplitKey(ms(tick)))
	assert.Nil(t, d.mu.splitFinder)
}

func TestDeciderCallsEnsureSafeSplitKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	intn := rand.New(rand.NewSource(11)).Intn

	var d Decider
	Init(&d, intn, func() float64 { return 1.0 })

	baseKey := keys.SystemSQLCodec.TablePrefix(51)
	for i := 0; i < 4; i++ {
		baseKey = encoding.EncodeUvarintAscending(baseKey, uint64(52+i))
	}
	c0 := func() roachpb.Span { return roachpb.Span{Key: append([]byte(nil), keys.MakeFamilyKey(baseKey, 1)...)} }
	c1 := func() roachpb.Span { return roachpb.Span{Key: append([]byte(nil), keys.MakeFamilyKey(baseKey, 9)...)} }

	expK, err := keys.EnsureSafeSplitKey(c1().Key)
	require.NoError(t, err)

	var k roachpb.Key
	var now time.Time
	for i := 0; i < 2*int(minSplitSuggestionInterval/time.Second); i++ {
		now = now.Add(500 * time.Millisecond)
		d.Record(now, 1, c0)
		now = now.Add(500 * time.Millisecond)
		d.Record(now, 1, c1)
		k = d.MaybeSplitKey(now)
		if len(k) != 0 {
			break
		}
	}

	require.Equal(t, expK, k)
}

func TestDeciderIgnoresEnsureSafeSplitKeyOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	intn := rand.New(rand.NewSource(11)).Intn

	var d Decider
	Init(&d, intn, func() float64 { return 1.0 })

	baseKey := keys.SystemSQLCodec.TablePrefix(51)
	for i := 0; i < 4; i++ {
		baseKey = encoding.EncodeUvarintAscending(baseKey, uint64(52+i))
	}
	c0 := func() roachpb.Span {
		return roachpb.Span{Key: append([]byte(nil), encoding.EncodeUvarintAscending(baseKey, math.MaxInt32+1)...)}
	}
	c1 := func() roachpb.Span {
		return roachpb.Span{Key: append([]byte(nil), encoding.EncodeUvarintAscending(baseKey, math.MaxInt32+2)...)}
	}

	_, err := keys.EnsureSafeSplitKey(c1().Key)
	require.Error(t, err)

	var k roachpb.Key
	var now time.Time
	for i := 0; i < 2*int(minSplitSuggestionInterval/time.Second); i++ {
		now = now.Add(500 * time.Millisecond)
		d.Record(now, 1, c0)
		now = now.Add(500 * time.Millisecond)
		d.Record(now, 1, c1)
		k = d.MaybeSplitKey(now)
		if len(k) != 0 {
			break
		}
	}

	require.Equal(t, c1().Key, k)
}
