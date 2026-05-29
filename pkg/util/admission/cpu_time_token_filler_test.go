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
	"github.com/cockroachdb/crlib/crtime"
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
	tokens    int64
	burstFrac float64
}

func (m *testBurstManager) refillGroupBurstBuckets(rate, capacity float64) {
	toAdd := int64(rate * m.burstFrac)
	groupCap := int64(capacity * m.burstFrac)
	m.tokens += toAdd
	if m.tokens > groupCap {
		m.tokens = groupCap
	}
	if m.tokens < -groupCap/4 {
		m.tokens = -groupCap / 4
	}
}

func (m *testModel) init() {}

func (m *testModel) fit(_ context.Context, targets targetUtilizations) rates {
	round := func(x float64) int {
		scaled := x * 100
		return int(math.Round(scaled))
	}
	fmt.Fprint(m.buf, "fit(\n")
	for qual := int(numBurstQualifications - 1); qual >= 0; qual-- {
		fmt.Fprintf(m.buf, "\t%s -> %v%%\n",
			burstQualification(qual).String(), round(targets[qual]))
	}
	fmt.Fprint(m.buf, ")\n")
	return m.rates
}

func TestCPUTimeTokenAllocator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	metrics := makeCPUTimeTokenMetrics()
	granter := newCPUTimeTokenGranter(metrics, timeutil.DefaultTimeSource{})
	req := &testRequester{granter: granter}
	granter.requester = req

	var buf strings.Builder
	burstMgr := &testBurstManager{burstFrac: defaultTenantGroupConfig.BurstFrac}
	flushAndReset := func() string {
		fmt.Fprint(&buf, granter.String())
		fmt.Fprintf(&buf, "burstM  %d\n", burstMgr.tokens)
		str := buf.String()
		buf.Reset()
		return str
	}

	model := &testModel{buf: &buf}
	model.rates[canBurst] = 5000
	model.rates[noBurst] = 4000
	st := cluster.MakeClusterSettings()
	allocator := cpuTimeTokenAllocator{
		granter:         granter,
		settings:        st,
		configHolder:    newResourceGroupConfigHolder(&st.SV),
		model:           model,
		metrics:         metrics,
		queue:           burstMgr,
		nowMono:         crtime.NowMono,
		dampeningFactor: 1.0,
	}

	ctx := context.Background()
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cpu_time_token_allocator"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "resetInterval":
			var increaseRatesBy int64
			d.MaybeScanArgs(t, "increase_rates_by", &increaseRatesBy)
			if increaseRatesBy != 0 {
				model.rates[canBurst] += increaseRatesBy
				model.rates[noBurst] += increaseRatesBy
			}
			allocator.resetInterval(ctx)
			return flushAndReset()
		case "allocate":
			var remainingTicks int64
			d.ScanArgs(t, "remaining", &remainingTicks)
			allocator.allocateTokens(remainingTicks)
			return flushAndReset()
		case "set-tokens":
			var v int64
			d.ScanArgs(t, "v", &v)
			granter.mu.buckets[canBurst].tokens = v
			granter.mu.buckets[noBurst].tokens = v
			burstMgr.tokens = v
			return flushAndReset()
		case "set-cpu-load":
			var runnable, procs int
			d.ScanArgs(t, "runnable", &runnable)
			d.ScanArgs(t, "procs", &procs)
			allocator.lastRunnable.Store(int64(runnable))
			allocator.lastProcs.Store(int64(procs))
			return fmt.Sprintf("cpu-load: runnable=%d procs=%d\n", runnable, procs)
		case "setClusterSettings":
			for _, arg := range d.CmdArgs {
				if arg.Key != "target" && arg.Key != "burst" {
					t.Fatalf("unknown setClusterSettings arg %q", arg.Key)
				}
			}
			ctx := context.Background()
			var override float64
			if d.MaybeScanArgs(t, "target", &override) {
				fmt.Fprintf(&buf, "SET CLUSTER SETTING admission.cpu_time_tokens.target_util = %v\n", override)
				KVCPUTimeUtilGoal.Override(ctx, &allocator.settings.SV, override)
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
		metrics:                  makeCPUTimeTokenMetrics(),
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
	targets[noBurst] = 0.8
	targets[canBurst] = 0.85

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
	require.Equal(t, int64(8000000000), refillRates[noBurst])
	// 85% util -> 10 vCPUs * .85 * 1s = 8.5s
	require.Equal(t, int64(8500000000), refillRates[canBurst])

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
	require.Equal(t, int64(2500000000), refillRates[noBurst])
	// 85% -> 10 vCPUs * .85 * 1s = 8.5s -> 8.5s / 3.2 = 2.65625s
	require.Equal(t, int64(2656250000), refillRates[canBurst])

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

// TestComputeTargets verifies that computeTargets derives target
// utilizations from a ConfigSnapshot.
func TestComputeTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	snap := ConfigSnapshot{
		noBurstFrac: 0.75,
		burstDelta:  0.25,
	}
	targets := computeTargets(snap)

	require.Equal(t, 0.75, targets[noBurst])
	require.Equal(t, 1.0, targets[canBurst])
}

// TestGroupBurstRates verifies the allocator's groupBurstRates method,
// which recovers the 100%-CPU rate from the canBurst allocation
// divided by canBurstTarget (read from the snapshot).
func TestGroupBurstRates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	st := cluster.MakeClusterSettings()
	ctx := context.Background()
	allocator := cpuTimeTokenAllocator{
		configHolder: newResourceGroupConfigHolder(&st.SV),
	}

	// canBurstTarget=1.0 recovers tokens/rates as-is.
	KVCPUTimeUtilGoal.Override(ctx, &st.SV, 0.5)
	KVCPUTimeUtilBurstDelta.Override(ctx, &st.SV, 0.5)
	var tokens tokenCounts
	tokens[canBurst] = 1000
	var rr rates
	rr[canBurst] = 4000
	rate100, cap100 := allocator.groupBurstRates(allocator.configHolder.Snapshot(), tokens, rr)
	require.Equal(t, 1000.0, rate100)
	require.Equal(t, 4000.0, cap100)

	// NoBurstFrac=0.5, BurstDelta=0.01 -> canBurstTarget=0.51, scales both.
	KVCPUTimeUtilGoal.Override(ctx, &st.SV, 0.5)
	KVCPUTimeUtilBurstDelta.Override(ctx, &st.SV, 0.01)
	rate100, cap100 = allocator.groupBurstRates(allocator.configHolder.Snapshot(), tokens, rr)
	require.InDelta(t, 1000.0/0.51, rate100, 0.01)
	require.InDelta(t, 4000.0/0.51, cap100, 0.01)

	// Delta path: tokens (rate100) may be negative when refill rates
	// decrease between intervals.
	KVCPUTimeUtilGoal.Override(ctx, &st.SV, 0.5)
	KVCPUTimeUtilBurstDelta.Override(ctx, &st.SV, 0.5)
	tokens[canBurst] = -500
	rr[canBurst] = 4000
	rate100, cap100 = allocator.groupBurstRates(allocator.configHolder.Snapshot(), tokens, rr)
	require.Equal(t, -500.0, rate100)
	require.Equal(t, 4000.0, cap100)
}

// TestDampeningFactor exercises the four regimes (descent, hold,
// recovery, snap-back), checks that dampening scales granter
// allocations without touching per-group burst buckets, and that the
// deficit counter stops growing once the factor is back at 1.0.
func TestDampeningFactor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	metrics := makeCPUTimeTokenMetrics()
	granter := newCPUTimeTokenGranter(metrics, timeutil.DefaultTimeSource{})
	granter.requester = &testRequester{granter: granter}
	burstMgr := &testBurstManager{burstFrac: defaultTenantGroupConfig.BurstFrac}

	st := cluster.MakeClusterSettings()
	model := &testModel{buf: &strings.Builder{}}
	model.rates[canBurst] = 5000
	model.rates[noBurst] = 4000
	// Use a manual monotonic clock so the deficit metric accumulates a
	// predictable number of nanoseconds per allocateTokens call.
	ts := timeutil.NewTestTimeSource()
	const tickInterval = time.Millisecond
	tickAndNow := func() crtime.Mono {
		ts.AdvanceBy(tickInterval)
		return ts.NowMono()
	}
	allocator := cpuTimeTokenAllocator{
		granter:         granter,
		settings:        st,
		configHolder:    newResourceGroupConfigHolder(&st.SV),
		model:           model,
		metrics:         metrics,
		queue:           burstMgr,
		nowMono:         tickAndNow,
		dampeningFactor: 1.0,
	}

	ctx := context.Background()
	allocator.resetInterval(ctx)

	// With the default threshold=32 and procs=10: descend when runnable
	// >= 320, snap to 1.0 when runnable < 40, recover otherwise.
	allocator.lastProcs.Store(10)

	clearBuckets := func() {
		granter.mu.buckets[canBurst].tokens = 0
		granter.mu.buckets[noBurst].tokens = 0
		burstMgr.tokens = 0
	}

	// Descent. One tick at runnable=320: 1.0 -> 0.99.
	allocator.lastRunnable.Store(320)
	clearBuckets()
	allocator.allocateTokens(1)
	require.InDelta(t, 0.99, allocator.dampeningFactor, 0.001)
	// Granter allocations are scaled (5000*0.99); per-group burst is not.
	require.Equal(t, int64(4950), granter.mu.buckets[canBurst].tokens)
	require.Equal(t, int64(3960), granter.mu.buckets[noBurst].tokens)
	// Burst refill should NOT be dampened. It uses the undampened
	// allocations through groupBurstRates, which derives rate100 from
	// the canBurst allocation (5000 with default canBurstTarget=0.85).
	// Scaled by BurstFrac=0.20 => floor(5000/0.85*0.20) = 1176.
	require.Equal(t, int64(1176), burstMgr.tokens)

	// Drive to the floor.
	for i := 0; i < 100; i++ {
		allocator.resetInterval(ctx)
		clearBuckets()
		allocator.allocateTokens(1)
	}
	require.Equal(t, dampeningFloor, allocator.dampeningFactor)
	require.Equal(t, int64(2500), granter.mu.buckets[canBurst].tokens)

	// Hold. runnable=260 sits in [240, 320), so the factor must not move.
	allocator.lastRunnable.Store(260)
	for i := 0; i < 10; i++ {
		allocator.resetInterval(ctx)
		clearBuckets()
		allocator.allocateTokens(1)
	}
	require.Equal(t, dampeningFloor, allocator.dampeningFactor)
	require.Equal(t, int64(2500), granter.mu.buckets[canBurst].tokens)

	// Recovery. runnable=200 is between 40 and 240; one tick: 0.50 -> 0.52.
	allocator.lastRunnable.Store(200)
	allocator.resetInterval(ctx)
	clearBuckets()
	allocator.allocateTokens(1)
	require.InDelta(t, 0.52, allocator.dampeningFactor, 0.001)
	require.Equal(t, int64(2600), granter.mu.buckets[canBurst].tokens)

	// 30 more ticks at +0.02 covers the climb back to 1.0.
	for i := 0; i < 30; i++ {
		allocator.resetInterval(ctx)
		clearBuckets()
		allocator.allocateTokens(1)
	}
	require.Equal(t, 1.0, allocator.dampeningFactor)

	// Snap-back. Drive to the floor again, then drop runnable below 40.
	allocator.lastRunnable.Store(320)
	for i := 0; i < 60; i++ {
		allocator.resetInterval(ctx)
		clearBuckets()
		allocator.allocateTokens(1)
	}
	require.Equal(t, dampeningFloor, allocator.dampeningFactor)

	allocator.lastRunnable.Store(20)
	allocator.resetInterval(ctx)
	clearBuckets()
	allocator.allocateTokens(1)
	require.Equal(t, 1.0, allocator.dampeningFactor)
	require.Equal(t, int64(5000), granter.mu.buckets[canBurst].tokens)

	// Deficit was accumulated; with factor at 1.0 it stops growing.
	require.Positive(t, metrics.DampeningDeficitNanos.Count())
	deficitBefore := metrics.DampeningDeficitNanos.Count()
	for i := 0; i < 10; i++ {
		allocator.resetInterval(ctx)
		allocator.allocateTokens(1)
	}
	require.Equal(t, deficitBefore, metrics.DampeningDeficitNanos.Count())
}
