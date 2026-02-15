// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCPUTimeTokenFiller(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if time.Second%timePerTick != 0 || timePerTick > time.Second {
		t.Errorf("timePerTick=%v must be < 1s & must divide 1s evenly", timePerTick)
		return
	}

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

	ctx := context.Background()
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
			filler.start(ctx)
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

func (a *testTokenAllocator) resetInterval(context.Context) {
	fmt.Fprintf(a.buf, "resetInterval()\n")
}

func (a *testTokenAllocator) allocateTokens(remainingTicks int64) {
	fmt.Fprintf(a.buf, "allocateTokens(%d)\n", remainingTicks)
}

type testModel struct {
	buf   *strings.Builder
	rates rates
}

type testBurstManager struct {
	tokens int64
}

func (m *testBurstManager) refillBurstBuckets(toAdd int64, capacity int64) {
	m.tokens += toAdd
	if m.tokens > capacity {
		m.tokens = capacity
	}
}

func (m *testModel) init() {}

func (m *testModel) fit(_ context.Context, targets targetUtilizations) rates {
	// targets uses float64, which when written to golden file can lead to
	// test reproducibility issues. Here, we multiply by 100 & then round to
	// the nearest integer.
	round := func(x float64) int {
		scaled := x * 100
		return int(math.Round(scaled))
	}
	fmt.Fprint(m.buf, "fit(\n")
	for tier := int(numResourceTiers - 1); tier >= 0; tier-- {
		for qual := int(numBurstQualifications - 1); qual >= 0; qual-- {
			fmt.Fprintf(m.buf, "\ttier%d %s -> %v%%\n", tier, burstQualification(qual).String(), round(targets[tier][qual]))
		}
	}
	fmt.Fprint(m.buf, ")\n")
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

	var buf strings.Builder
	var printBurstMgrs func() string
	flushAndReset := func() string {
		fmt.Fprint(&buf, granter.String())
		fmt.Fprint(&buf, printBurstMgrs())
		str := buf.String()
		buf.Reset()
		return str
	}

	model := &testModel{buf: &buf}
	model.rates[testTier0][canBurst] = 5000
	model.rates[testTier0][noBurst] = 4000
	model.rates[testTier1][canBurst] = 3000
	model.rates[testTier1][noBurst] = 2000
	burstMgrs := [numResourceTiers]*testBurstManager{
		testTier0: {},
		testTier1: {},
	}
	allocator := cpuTimeTokenAllocator{
		granter:  granter,
		settings: cluster.MakeClusterSettings(),
		model:    model,
		queues: [numResourceTiers]workQueueIForAllocator{
			testTier0: burstMgrs[testTier0],
			testTier1: burstMgrs[testTier1],
		},
	}
	printBurstMgrs = func() string {
		var b strings.Builder
		fmt.Fprintf(&b, "burstM\n")
		fmt.Fprintf(&b, "tier0  %d\n", burstMgrs[testTier0].tokens)
		fmt.Fprintf(&b, "tier1  %d\n", burstMgrs[testTier1].tokens)
		return b.String()
	}

	ctx := context.Background()
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cpu_time_token_allocator"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "resetInterval":
			var increaseRatesBy int64
			d.MaybeScanArgs(t, "increase_rates_by", &increaseRatesBy)
			if increaseRatesBy != 0 {
				model.rates[testTier0][canBurst] += increaseRatesBy
				model.rates[testTier0][noBurst] += increaseRatesBy
				model.rates[testTier1][canBurst] += increaseRatesBy
				model.rates[testTier1][noBurst] += increaseRatesBy
			}
			allocator.resetInterval(ctx)
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
			burstMgrs[testTier0].tokens = 0
			burstMgrs[testTier1].tokens = 0
			return flushAndReset()
		case "setClusterSettings":
			ctx := context.Background()
			var override float64
			if d.MaybeScanArgs(t, "app", &override) {
				fmt.Fprintf(&buf, "SET CLUSTER SETTING admission.cpu_time_tokens.target_util.app_tenant = %v\n", override)
				KVCPUTimeAppUtilGoal.Override(ctx, &allocator.settings.SV, override)
			}
			if d.MaybeScanArgs(t, "system", &override) {
				fmt.Fprintf(&buf, "SET CLUSTER SETTING admission.cpu_time_tokens.target_util.system_tenant = %v\n", override)
				KVCPUTimeSystemUtilGoal.Override(ctx, &allocator.settings.SV, override)
			}
			if d.MaybeScanArgs(t, "burst", &override) {
				fmt.Fprintf(&buf, "SET CLUSTER SETTING admission.cpu_time_tokens.target_util.burst_delta = %v\n", override)
				KVCPUTimeUtilBurstDelta.Override(ctx, &allocator.settings.SV, override)
			}
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
		timeSource:               testTime,
		lastFitTime:              testTime.Now(),
		totalCPUTime:             0,
		tokenToCPUTimeMultiplier: 1,
	}
	tokenCPUTime := &testTokenUsageTracker{}
	model.granter = tokenCPUTime
	actualCPUTime := &testCPUMetricsProvider{
		capacity: 10,
	}
	model.cpuMetricsProvider = actualCPUTime

	dur := 5 * time.Second
	actualCPUTime.append(dur, 1) // appended value ignored by init

	var targets targetUtilizations
	targets[testTier1][noBurst] = 0.8
	targets[testTier1][canBurst] = 0.85
	targets[testTier0][noBurst] = 0.9
	targets[testTier0][canBurst] = 0.95

	// The first call to fit inits the model, by setting tokenToCPUTimeMultiplier
	// to one, since in prod on the first call to fit, there will be no CPU
	// usage data to use to determine tokenToCPUTimeMultiplier.
	ctx := context.Background()
	refillRates := model.fit(ctx, targets)
	require.Equal(t, float64(1), model.tokenToCPUTimeMultiplier)
	// Given that tokenToCPUTimeMultiplier equals one, refillRates is equal
	// to target utilization for the bucket * the vCPU count (10 vCPUs in this
	// test). The unit of refillRates is nanoseconds.
	//
	// 80% util -> 10 vCPUs * .8 * 1s = 8s
	require.Equal(t, int64(8000000000), refillRates[testTier1][noBurst])
	// 85% util -> 10 vCPUs * .85 * 1s = 8.5s
	require.Equal(t, int64(8500000000), refillRates[testTier1][canBurst])
	// 90% util -> 10 vCPUs * .9 * 1s = 9s
	require.Equal(t, int64(9000000000), refillRates[testTier0][noBurst])
	// 95% util -> 10 vCPUs * .95 * 1s = 9.5s
	require.Equal(t, int64(9500000000), refillRates[testTier0][canBurst])

	// Below tests are of the computation of tokenToCPUTimeMultiplier only. The
	// computation of tokenToCPUTimeMultiplier involves state stored on the model,
	// since the model does exponential smoothing. The computation of refillRates
	// (given a fixed tokenToCPUTimeMultiplier) is simpler: It is a pure function,
	// described up above in the test case of the first call to fit. So here we
	// focus on tokenToCPUTimeMultiplier.
	//
	// 2x
	// Token time is half of actual time, so tokenToCPUTimeMultiplier is two.
	// 100 data points are appended, to give the filter time to converge on two.
	tokenCPUTime.append(dur.Nanoseconds()/2, 100)
	actualCPUTime.append(dur, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		_ = model.fit(ctx, targets)
	}
	tolerance := 0.01
	require.InDelta(t, 2, model.tokenToCPUTimeMultiplier, tolerance)

	// 4x
	// Token time is one fourth of actual time, so tokenToCPUTimeMultiplier is
	// four.
	tokenCPUTime.append(dur.Nanoseconds()/2, 100)
	actualCPUTime.append(dur*2, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		_ = model.fit(ctx, targets)
	}
	require.InDelta(t, 4, model.tokenToCPUTimeMultiplier, tolerance)

	// 1x
	// Token time is one equal to actual time, so tokenToCPUTimeMultiplier is one.
	tokenCPUTime.append(dur.Nanoseconds()*2, 100)
	actualCPUTime.append(dur*2, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		_ = model.fit(ctx, targets)
	}
	require.InDelta(t, 1, model.tokenToCPUTimeMultiplier, tolerance)

	// 20x
	// tokenToCPUTimeMultiplier should be 40, based on the data, but the model caps
	// tokenToCPUTimeMultiplier at 20.
	tokenCPUTime.append(dur.Nanoseconds(), 100)
	actualCPUTime.append(dur*40, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		_ = model.fit(ctx, targets)
	}
	require.InDelta(t, 20, model.tokenToCPUTimeMultiplier, tolerance)

	// 1x
	// tokenToCPUTimeMultiplier should be 0.5, based on the data, but the model caps
	// tokenToCPUTimeMultiplier at 1.
	tokenCPUTime.append(dur.Nanoseconds()*2, 100)
	actualCPUTime.append(dur, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		_ = model.fit(ctx, targets)
	}
	require.InDelta(t, 1, model.tokenToCPUTimeMultiplier, tolerance)

	// 2x
	// Token time is half of actual time, so tokenToCPUTimeMultiplier is two.
	tokenCPUTime.append(dur.Nanoseconds(), 100)
	actualCPUTime.append(dur*2, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		_ = model.fit(ctx, targets)
	}
	require.InDelta(t, 2, model.tokenToCPUTimeMultiplier, tolerance)

	// Below tests are of the low CPU logic. See the comments in fit for a full
	// explanation of the logic & especially the rationale for the logic. TLDR:
	// if CPU is less than 25%, and if tokenToCPUTimeMultiplier is less 3.6,
	// tokenToCPUTimeMultiplier is left alone. If tokenToCPUTimeMultiplier is
	// greater than 3.6, tokenToCPUTimeMultiplier is divided by 1.5 until it is
	// <= 3.6.
	//
	// vCPU count is 10. dur /.5 = 1s. 1s / 10s = 0.1 < 0.25. So low CPU mode
	// should be activated.
	//
	// Leave existing tokenToCPUTimeMultiplier multiplier as is, since 2 <= 3.6.
	tokenCPUTime.append(dur.Nanoseconds()/5, 100)
	actualCPUTime.append(dur/5, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		_ = model.fit(ctx, targets)
	}
	require.InDelta(t, 2, model.tokenToCPUTimeMultiplier, tolerance)

	// Leave low vCPU mode, in order to set tokenToCPUTimeMultiplier equal to 20,
	// which is set up for the next test case.
	tokenCPUTime.append(dur.Nanoseconds(), 100)
	actualCPUTime.append(dur*100, 100)
	for i := 0; i < 100; i++ {
		testTime.Advance(time.Second)
		_ = model.fit(ctx, targets)
	}
	require.InDelta(t, 20, model.tokenToCPUTimeMultiplier, tolerance)

	// Iteratively reduce to 3.2x, since low CPU mode, and
	// tokenToCPUTimeMultiplier = 20 > 3.2. Why 3.2? First, fit computes
	// the smallest target from targets. In this case, that is 0.8. Then
	// the following formula is used to determine the upper bound for the
	// multiplier:
	// upperBound = smallestTargetUtil / lowCPUUtilFrac
	//            = 0.8 / 0.25
	//            = 3.2
	tokenCPUTime.append(dur.Nanoseconds()/5, 100)
	actualCPUTime.append(dur/5, 100)
	{
		lastMult := model.tokenToCPUTimeMultiplier
		for i := 0; ; i++ {
			require.Less(t, i, 100)
			testTime.Advance(time.Second)
			refillRates = model.fit(ctx, targets)
			mult := model.tokenToCPUTimeMultiplier
			if mult == lastMult {
				break
			}
			require.Less(t, mult, lastMult)
			lastMult = mult
		}
	}
	require.InDelta(t, 3.2, model.tokenToCPUTimeMultiplier, tolerance)

	// Check refillRates again, this time with tokenToCPUTimeMultiplier
	// equal to 3.2 instead of one.
	//
	// 80% -> 10 vCPUs * .8 * 1s = 8s -> 8s / 3.2 = 2.5s
	require.Equal(t, int64(2500000000), refillRates[testTier1][noBurst])
	// 85% -> 10 vCPUs * .85 * 1s = 8.5s -> 8.5s / 3.2 = 2.65625s
	require.Equal(t, int64(2656250000), refillRates[testTier1][canBurst])
	// 90% -> 10 vCPUs * .9 * 1s = 9s -> 9s / 3.2 = 2.8125s
	require.Equal(t, int64(2812500000), refillRates[testTier0][noBurst])
	// 95% -> 10 vCPUs * .95 * 1s = 9.5s -> 9.5s / 3.2 = 2.96875s
	require.Equal(t, int64(2968750000), refillRates[testTier0][canBurst])

	// We do not expect the syscall that fetches CPU usage to ever fail.
	// Verify that log.Fatalf is called when GetCPUUsage returns an error.
	actualCPUTime.retErr = errors.New("test goes boom")
	var exited bool
	log.SetExitFunc(true /* hideStack */, func(_ exit.Code) {
		exited = true
	})
	defer log.ResetExitFunc()
	_ = model.fit(ctx, targets)
	require.True(t, exited, "expected log.Fatalf to be called")
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

func (t *testTokenUsageTracker) resetTokensUsedInInterval() int64 {
	ret := t.tokensUsed[t.i]
	t.i++
	return ret
}

type testCPUMetricsProvider struct {
	i          int
	cumulative time.Duration
	durations  []time.Duration
	capacity   float64
	retErr     error
}

func (p *testCPUMetricsProvider) GetCPUUsage() (totalCPUTime time.Duration, err error) {
	if p.retErr != nil {
		return 0, p.retErr
	}
	cycle := p.durations[p.i]
	p.i++
	p.cumulative += cycle
	return p.cumulative, nil
}

func (p *testCPUMetricsProvider) GetCPUCapacity() (cpuCapacity float64) {
	return p.capacity
}

func (p *testCPUMetricsProvider) append(dur time.Duration, count int) {
	for i := 0; i < count; i++ {
		p.durations = append(p.durations, dur)
	}
}
