// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestCPUTimeTokenFiller(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Fixed time for reproducibility.
	unixNanos := int64(1758938600000000000) // 2025-09-24T14:30:00Z
	startTime := time.Unix(0, unixNanos).UTC()
	testTime := timeutil.NewManualTime(startTime)

	var buf strings.Builder
	allocator := testTokenAllocator{buf: &buf}
	var filler cpuTimeTokenFiller
	flushAndReset := func() string {
		fmt.Fprintf(&buf, "elapsed: %s\n", testTime.Since(startTime))
		str := buf.String()
		buf.Reset()
		return str
	}

	tickCh := make(chan struct{})
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cpu_time_token_filler"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			filler = cpuTimeTokenFiller{
				allocator:  &allocator,
				closeCh:    make(chan struct{}),
				timeSource: testTime,
				tickCh:     &tickCh,
			}
			filler.start()
			return flushAndReset()
		case "advance":
			var dur time.Duration
			d.ScanArgs(t, "dur", &dur)
			testTime.AdvanceInOneTick(dur)
			<-tickCh
			return flushAndReset()
		case "stop":
			close(filler.closeCh)
			return flushAndReset()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

type testTokenAllocator struct {
	buf *strings.Builder
}

func (m *testTokenAllocator) init() {}

func (a *testTokenAllocator) resetInterval(skipFittingLinearModel bool) {
	fmt.Fprintf(a.buf, "resetInterval(%t)\n", skipFittingLinearModel)
}

func (a *testTokenAllocator) allocateTokens(remainingTicks int64) {
	fmt.Fprintf(a.buf, "allocateTokens(%d)\n", remainingTicks)
}

type testModel struct {
	rates [numResourceTiers][numBurstQualifications]int64
	delta [numResourceTiers][numBurstQualifications]int64
}

func (m *testModel) init() {}

func (m *testModel) fit() [numResourceTiers][numBurstQualifications]int64 {
	return m.delta
}

func (m *testModel) getRefillRates() [numResourceTiers][numBurstQualifications]int64 {
	return m.rates
}

func TestCPUTimeTokenAllocator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	granter := &cpuTimeTokenGranter{}
	tier0Granter := &cpuTimeTokenChildGranter{
		tier:   testTier0,
		parent: granter,
	}
	tier1Granter := &cpuTimeTokenChildGranter{
		tier:   testTier1,
		parent: granter,
	}
	var requesters [numResourceTiers]*testRequester
	requesters[testTier0] = &testRequester{
		additionalID: "tier0",
		granter:      tier0Granter,
	}
	requesters[testTier1] = &testRequester{
		additionalID: "tier1",
		granter:      tier1Granter,
	}
	granter.requester[testTier0] = requesters[testTier0]
	granter.requester[testTier1] = requesters[testTier1]

	model := &testModel{}
	model.rates[testTier0][canBurst] = 5
	model.rates[testTier0][noBurst] = 4
	model.rates[testTier1][canBurst] = 3
	model.rates[testTier1][noBurst] = 2
	allocator := cpuTimeTokenAllocator{
		granter: granter,
		model:   model,
	}

	var buf strings.Builder
	flushAndReset := func() string {
		fmt.Fprint(&buf, granter.String())
		str := buf.String()
		buf.Reset()
		return str
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cpu_time_token_allocator"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "resetInterval":
			var delta int64
			d.MaybeScanArgs(t, "delta", &delta)
			if d.MaybeScanArgs(t, "delta", &delta) {
				for tier := range model.delta {
					for qual := range model.delta[tier] {
						model.delta[tier][qual] = delta
					}
				}
			}
			skipFit := d.HasArg("skipfit")
			allocator.resetInterval(skipFit /* skipFittingLinearModel */)
			for tier := range model.delta {
				for qual := range model.delta[tier] {
					model.delta[tier][qual] = 0
				}
			}
			return flushAndReset()
		case "allocate":
			var remainingTicks int64
			d.ScanArgs(t, "remaining", &remainingTicks)
			allocator.allocateTokens(remainingTicks)
			return flushAndReset()
		case "clear":
			granter.mu.buckets[testTier0][canBurst].tokens = 0
			granter.mu.buckets[testTier0][noBurst].tokens = 0
			granter.mu.buckets[testTier1][canBurst].tokens = 0
			granter.mu.buckets[testTier1][noBurst].tokens = 0
			return flushAndReset()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestCPUTimeTokenLinearModel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	unixNanos := int64(1758938600000000000) // 2025-09-24T14:30:00Z
	testTime := timeutil.NewManualTime(time.Unix(0, unixNanos).UTC())
	model := cpuTimeTokenLinearModel{
		settings:                 cluster.MakeClusterSettings(),
		timeSource:               testTime,
		lastFitTime:              testTime.Now(),
		totalCPUTimeMillis:       0,
		tokenToCPUTimeMultiplier: 1,
	}
	tokenCPUTime := &testTokenUsageTracker{}
	model.granter = tokenCPUTime
	actualCPUTime := &testCPUMetricProvider{
		capacity: 10,
	}
	model.cpuMetricProvider = actualCPUTime

	dur := 5 * time.Second
	actualCPUTime.append(dur.Nanoseconds(), 1) // appended value ignored by init
	model.init()

	// 2x.
	tokenCPUTime.append(dur.Nanoseconds()/2, 100)
	actualCPUTime.append(dur.Milliseconds(), 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		model.fit()
	}
	tolerance := 0.01
	require.InDelta(t, 2, model.tokenToCPUTimeMultiplier, tolerance)

	// 4x.
	tokenCPUTime.append(dur.Nanoseconds()/2, 100)
	actualCPUTime.append(dur.Milliseconds()*2, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		model.fit()
	}
	require.InDelta(t, 4, model.tokenToCPUTimeMultiplier, tolerance)

	// 1x.
	tokenCPUTime.append(dur.Nanoseconds()*2, 100)
	actualCPUTime.append(dur.Milliseconds()*2, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		model.fit()
	}
	require.InDelta(t, 1, model.tokenToCPUTimeMultiplier, tolerance)

	// Cap at 20x.
	tokenCPUTime.append(dur.Nanoseconds(), 100)
	actualCPUTime.append(dur.Milliseconds()*40, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		model.fit()
	}
	require.InDelta(t, 20, model.tokenToCPUTimeMultiplier, tolerance)

	// Cap at 1x.
	tokenCPUTime.append(dur.Nanoseconds()*2, 100)
	actualCPUTime.append(dur.Milliseconds(), 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		model.fit()
	}
	require.InDelta(t, 1, model.tokenToCPUTimeMultiplier, tolerance)

	// 2x.
	tokenCPUTime.append(dur.Nanoseconds(), 100)
	actualCPUTime.append(dur.Milliseconds()*2, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		model.fit()
	}
	require.InDelta(t, 2, model.tokenToCPUTimeMultiplier, tolerance)

	// Leave 2x as is, even tho low CPU mode, since multiplier is already low.
	tokenCPUTime.append(dur.Nanoseconds()/5, 100)
	actualCPUTime.append(dur.Milliseconds()/5, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		model.fit()
	}
	require.InDelta(t, 2, model.tokenToCPUTimeMultiplier, tolerance)

	// 20x.
	tokenCPUTime.append(dur.Nanoseconds(), 100)
	actualCPUTime.append(dur.Milliseconds()*100, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		model.fit()
	}
	require.InDelta(t, 20, model.tokenToCPUTimeMultiplier, tolerance)

	// Reduce to 3.6x, since low CPU mode, and multiplier is high.
	tokenCPUTime.append(dur.Nanoseconds()/5, 100)
	actualCPUTime.append(dur.Milliseconds()/5, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		model.fit()
	}
	require.InDelta(t, 3.6, model.tokenToCPUTimeMultiplier, tolerance)

	rates := model.getRefillRates()
	// Hard-coded to be 0.
	// 95% -> 10 vCPUs * .95 * 1s = 9.5s, 9.5s / 3.6 ~= 2.63888889
	require.Equal(t, int64(2638888888), rates[testTier0][canBurst])
	// 90% -> 10 vCPUs * .9 * 1s = 9s, 9s / 3.6 ~= 2.5s
	require.Equal(t, int64(2500000000), rates[testTier0][noBurst])
	// 85% -> 10 vCPUs * .85 * 1s = 8.5s, 8.5s / 3.6 ~= 2.36111111s
	require.Equal(t, int64(2361111111), rates[testTier1][canBurst])
	// 80% -> 10 vCPUs * .8 * 1s = 8s, 8s / 3.6 ~= 2.22222222s
	require.Equal(t, int64(2222222222), rates[testTier1][noBurst])
}

type testTokenUsageTracker struct {
	i          int
	tokensUsed []int64
}

func (t *testTokenUsageTracker) append(tokens int64, count int) {
	for i := 0; i < count; i++ {
		t.tokensUsed = append(t.tokensUsed, tokens)
	}
}

func (t *testTokenUsageTracker) getTokensUsedInInterval() int64 {
	ret := t.tokensUsed[t.i]
	t.i++
	return ret
}

type testCPUMetricProvider struct {
	i        int
	cum      int64
	millis   []int64
	capacity float64
}

func (m *testCPUMetricProvider) GetCPUInfo() (int64, float64) {
	cycle := m.millis[m.i]
	m.i++
	m.cum += cycle
	return m.cum, m.capacity
}

func (t *testCPUMetricProvider) append(millis int64, count int) {
	for i := 0; i < count; i++ {
		t.millis = append(t.millis, millis)
	}
}
