// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// TestIOLoadListener is a datadriven test with the following command that
// sets the state for token calculation and then ticks adjustmentInterval
// times to cause tokens to be set in the testGranterWithIOTokens:
// set-state admitted=<int> l0-bytes=<int> l0-added=<int> l0-files=<int> l0-sublevels=<int> ...
func TestIOLoadListener(t *testing.T) {
	req := &testRequesterForIOLL{}
	kvGranter := &testGranterWithIOTokens{}
	var ioll *ioLoadListener
	var cumFlushBytes int64
	var cumFlushWork, cumFlushIdle time.Duration
	var cumWALSecondaryWriteDuration time.Duration

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "io_load_listener"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				L0MinimumSizePerSubLevel.Override(ctx, &st.SV, 0)
				walFailoverUnlimitedTokens.Override(ctx, &st.SV, false)
				ElasticBandwidthMaxUtil.Override(ctx, &st.SV, 1)
				ioll = &ioLoadListener{
					settings:              st,
					kvRequester:           req,
					perWorkTokenEstimator: makeStorePerWorkTokenEstimator(),
					diskBandwidthLimiter:  newDiskBandwidthLimiter(),
					l0CompactedBytes:      metric.NewCounter(l0CompactedBytes),
					l0TokensProduced:      metric.NewCounter(l0TokensProduced),
				}
				// The mutex is needed by ioLoadListener but is not useful in this
				// test -- the channels provide synchronization and prevent this
				// test code and the ioLoadListener from being concurrently
				// active.
				ioll.kvGranter = kvGranter

				// Reset the cumulative data
				cumFlushBytes = 0
				cumFlushWork = time.Duration(0)
				cumFlushIdle = time.Duration(0)
				cumWALSecondaryWriteDuration = 0
				return ""

			case "prep-admission-stats":
				req.stats = storeAdmissionStats{
					workCount:              0,
					writeAccountedBytes:    0,
					ingestedAccountedBytes: 0,
				}
				d.ScanArgs(t, "admitted", &req.stats.workCount)
				if d.HasArg("write-bytes") {
					d.ScanArgs(t, "write-bytes", &req.stats.writeAccountedBytes)
				}
				if d.HasArg("ingested-bytes") {
					d.ScanArgs(t, "ingested-bytes", &req.stats.ingestedAccountedBytes)
				}
				belowRaft := false
				if d.HasArg("below-raft") {
					d.ScanArgs(t, "below-raft", &belowRaft)
				}
				if !belowRaft {
					req.stats.aboveRaftStats.workCount = req.stats.workCount
					req.stats.aboveRaftStats.writeAccountedBytes = req.stats.writeAccountedBytes
					req.stats.aboveRaftStats.ingestedAccountedBytes = req.stats.ingestedAccountedBytes
				}
				return fmt.Sprintf("%+v", req.stats)

			case "set-min-flush-util":
				var percent int
				d.ScanArgs(t, "percent", &percent)
				MinFlushUtilizationFraction.Override(ctx, &st.SV, float64(percent)/100)
				return ""

			case "set-unlimited-wal-failover-tokens":
				unlimitedTokensEnabled := true
				if d.HasArg("enabled") {
					d.ScanArgs(t, "enabled", &unlimitedTokensEnabled)
				}
				walFailoverUnlimitedTokens.Override(ctx, &st.SV, unlimitedTokensEnabled)
				return ""

			case "set-min-size-per-sub-level":
				var minSize int64
				d.ScanArgs(t, "size", &minSize)
				L0MinimumSizePerSubLevel.Override(ctx, &st.SV, minSize)
				return ""

			// TODO(sumeer): the output printed by set-state is hard to follow. It
			// prints the internal fields which are hard to interpret, and it prints
			// a properly formatted ioLoadListenerState. The latter is supposed to
			// be easier to understand, but reviewers have noted that it is still
			// challenging to understand whether the output is correct. Come up with
			// more easily consumable output. Additionally, the input uses
			// cumulative values, so one has to look at the preceding testdata -- we
			// could instead accept the interval delta as input.
			case "set-state":
				// Setup state used as input for token adjustment.
				var metrics pebble.Metrics
				var l0Bytes uint64
				d.ScanArgs(t, "l0-bytes", &l0Bytes)
				metrics.Levels[0].Size = int64(l0Bytes)
				var l0AddedWrite, l0AddedIngested uint64
				d.ScanArgs(t, "l0-added-write", &l0AddedWrite)
				metrics.Levels[0].BytesFlushed = l0AddedWrite
				if d.HasArg("l0-added-ingested") {
					d.ScanArgs(t, "l0-added-ingested", &l0AddedIngested)
				}
				metrics.Levels[0].BytesIngested = l0AddedIngested
				var l0Files int
				d.ScanArgs(t, "l0-files", &l0Files)
				metrics.Levels[0].NumFiles = int64(l0Files)
				var l0SubLevels int
				d.ScanArgs(t, "l0-sublevels", &l0SubLevels)
				metrics.Levels[0].Sublevels = int32(l0SubLevels)
				var flushBytes, flushWorkSec, flushIdleSec int
				if d.HasArg("flush-bytes") {
					d.ScanArgs(t, "flush-bytes", &flushBytes)
					d.ScanArgs(t, "flush-work-sec", &flushWorkSec)
					d.ScanArgs(t, "flush-idle-sec", &flushIdleSec)
				} else {
					// Make flush utilization low, but keep peak throughput sane by
					// setting flushWorkSec > 0.
					flushWorkSec = 1
					flushIdleSec = 100
				}
				if d.HasArg("base-level") {
					var baseLevel int
					d.ScanArgs(t, "base-level", &baseLevel)
					metrics.Levels[baseLevel].Size = 1000
					var compactedBytes int
					d.ScanArgs(t, "compacted-bytes", &compactedBytes)
					metrics.Levels[baseLevel].BytesCompacted = uint64(compactedBytes)
				}

				cumFlushIdle += time.Duration(flushIdleSec) * time.Second
				cumFlushWork += time.Duration(flushWorkSec) * time.Second
				cumFlushBytes += int64(flushBytes)

				metrics.Flush.WriteThroughput.Bytes = cumFlushBytes
				metrics.Flush.WriteThroughput.IdleDuration = cumFlushIdle
				metrics.Flush.WriteThroughput.WorkDuration = cumFlushWork

				if d.HasArg("wal-secondary-write-sec") {
					var writeDurSec int
					d.ScanArgs(t, "wal-secondary-write-sec", &writeDurSec)
					cumWALSecondaryWriteDuration += time.Duration(writeDurSec) * time.Second
				}
				metrics.WAL.Failover.SecondaryWriteDuration = cumWALSecondaryWriteDuration

				var writeStallCount int
				if d.HasArg("write-stall-count") {
					d.ScanArgs(t, "write-stall-count", &writeStallCount)
				}
				var allTokensUsed bool
				if d.HasArg("all-tokens-used") {
					d.ScanArgs(t, "all-tokens-used", &allTokensUsed)
				}
				kvGranter.allTokensUsed = allTokensUsed
				var provisionedBandwidth, bytesRead, bytesWritten int
				if d.HasArg("provisioned-bandwidth") {
					d.ScanArgs(t, "provisioned-bandwidth", &provisionedBandwidth)
				}
				if d.HasArg("bytes-read") {
					d.ScanArgs(t, "bytes-read", &bytesRead)
				}
				if d.HasArg("bytes-written") {
					d.ScanArgs(t, "bytes-written", &bytesWritten)
				}
				if d.HasArg("disk-write-tokens-used") {
					var regularTokensUsed, elasticTokensUsed int64
					d.ScanArgs(t, "disk-write-tokens-used", &regularTokensUsed, &elasticTokensUsed)
					kvGranter.diskBandwidthTokensUsed[admissionpb.RegularWorkClass] = diskTokens{writeByteTokens: regularTokensUsed}
					kvGranter.diskBandwidthTokensUsed[admissionpb.ElasticWorkClass] = diskTokens{writeByteTokens: elasticTokensUsed}
				} else {
					kvGranter.diskBandwidthTokensUsed[admissionpb.RegularWorkClass] = diskTokens{}
					kvGranter.diskBandwidthTokensUsed[admissionpb.ElasticWorkClass] = diskTokens{}
				}
				var printOnlyFirstTick bool
				if d.HasArg("print-only-first-tick") {
					d.ScanArgs(t, "print-only-first-tick", &printOnlyFirstTick)
				}
				currDuration := unloadedDuration
				if d.HasArg("loaded") {
					currDuration = loadedDuration
				}
				memTableSizeForStopWrites := uint64(256 << 20)
				if d.HasArg("unflushed-too-large") {
					metrics.MemTable.Size = memTableSizeForStopWrites + 1
				}
				ioll.pebbleMetricsTick(ctx, StoreMetrics{
					Metrics:         &metrics,
					WriteStallCount: int64(writeStallCount),
					DiskStats: DiskStats{
						BytesRead:            uint64(bytesRead),
						BytesWritten:         uint64(bytesWritten),
						ProvisionedBandwidth: int64(provisionedBandwidth),
					},
					MemTableSizeForStopWrites: memTableSizeForStopWrites,
				})
				var buf strings.Builder
				// Do the ticks until just before next adjustment.
				res := ioll.adjustTokensResult
				fmt.Fprintln(&buf, redact.StringWithoutMarkers(&res))
				fmt.Fprintln(&buf, redact.StringWithoutMarkers(ioll.diskBandwidthLimiter))
				res.ioThreshold = nil // avoid nondeterminism
				fmt.Fprintf(&buf, "%+v\n", (rawTokenResult)(res))
				if req.buf.Len() > 0 {
					fmt.Fprintf(&buf, "%s\n", req.buf.String())
					req.buf.Reset()
				}
				for i := 0; i < int(currDuration.ticksInAdjustmentInterval()); i++ {
					ioll.allocateTokensTick(currDuration.ticksInAdjustmentInterval() - int64(i))
					if i == 0 || !printOnlyFirstTick {
						fmt.Fprintf(&buf, "tick: %d, %s\n", i, kvGranter.buf.String())
					}
					kvGranter.buf.Reset()
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestIOLoadListenerOverflow(t *testing.T) {
	req := &testRequesterForIOLL{}
	kvGranter := &testGranterWithIOTokens{}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	ioll := ioLoadListener{
		settings:         st,
		kvRequester:      req,
		l0CompactedBytes: metric.NewCounter(l0CompactedBytes),
		l0TokensProduced: metric.NewCounter(l0TokensProduced),
	}
	ioll.kvGranter = kvGranter
	// Bug 1: overflow when totalNumByteTokens is too large.
	for i := int64(0); i < adjustmentInterval; i++ {
		// Override the totalNumByteTokens manually to trigger the overflow bug.
		ioll.totalNumByteTokens = math.MaxInt64 - i
		ioll.byteTokensAllocated = 0
		for j := 0; j < int(unloadedDuration.ticksInAdjustmentInterval()); j++ {
			ioll.allocateTokensTick(unloadedDuration.ticksInAdjustmentInterval() - i)
		}
	}
	// Bug2: overflow when bytes added delta is 0.
	m := pebble.Metrics{}
	m.Levels[0] = pebble.LevelMetrics{
		Sublevels: 100,
		NumFiles:  10000,
	}
	ioll.pebbleMetricsTick(ctx, StoreMetrics{Metrics: &m})
	ioll.pebbleMetricsTick(ctx, StoreMetrics{Metrics: &m})
	ioll.allocateTokensTick(unloadedDuration.ticksInAdjustmentInterval())
}

// TODO(sumeer): we now do more work outside adjustTokensInner, so the parts
// of the adjustTokensResult computed by adjustTokensInner has become a subset
// of what is logged below, and the rest is logged with 0 values. Expand this
// test to call adjustTokens.
func TestAdjustTokensInnerAndLogging(t *testing.T) {
	const mb = 12 + 1<<20
	tests := []struct {
		name      redact.SafeString
		prev      ioLoadListenerState
		l0Metrics pebble.LevelMetrics
	}{
		{
			name: "zero",
		},
		{
			name: "real-numbers",
			prev: ioLoadListenerState{
				cumL0AddedBytes:              1402 * mb,
				curL0Bytes:                   400 * mb,
				cumWriteStallCount:           10,
				smoothedIntL0CompactedBytes:  47 * mb,
				smoothedCompactionByteTokens: 201 * mb,
				totalNumByteTokens:           int64(201 * mb),
			},
			l0Metrics: pebble.LevelMetrics{
				Sublevels:     27,
				NumFiles:      195,
				Size:          900 * mb,
				BytesIngested: 1801 * mb,
				BytesFlushed:  178 * mb,
			},
		},
	}
	ctx := context.Background()
	var buf redact.StringBuilder
	for _, tt := range tests {
		buf.Printf("%s:\n", tt.name)
		ioll := &ioLoadListener{
			settings:         cluster.MakeTestingClusterSettings(),
			l0CompactedBytes: metric.NewCounter(l0CompactedBytes),
			l0TokensProduced: metric.NewCounter(l0TokensProduced),
		}
		res := ioll.adjustTokensInner(
			ctx, tt.prev, tt.l0Metrics, 12, cumStoreCompactionStats{numOutLevelsGauge: 1}, 0,
			pebble.ThroughputMetric{}, 100, 10, 0, 0.50, 10, 100)
		buf.Printf("%s\n", res)
	}
	echotest.Require(t, string(redact.Sprint(buf)), filepath.Join(datapathutils.TestDataPath(t, "format_adjust_tokens_stats.txt")))
}

// TestBadIOLoadListenerStats tests that bad stats (non-monotonic cumulative
// stats and negative values) don't cause panics or tokens to be negative.
func TestBadIOLoadListenerStats(t *testing.T) {
	var m pebble.Metrics
	var d DiskStats
	req := &testRequesterForIOLL{}
	ctx := context.Background()

	randomValues := func() {
		// Use uints, and cast so that we get bad negative values.
		m.Levels[0].Sublevels = int32(rand.Uint32())
		m.Levels[0].NumFiles = int64(rand.Uint64())
		m.Levels[0].Size = int64(rand.Uint64())
		m.Levels[0].BytesFlushed = rand.Uint64()
		for i := range m.Levels {
			m.Levels[i].BytesIngested = rand.Uint64()
		}
		d.BytesRead = rand.Uint64()
		d.BytesWritten = rand.Uint64()
		d.ProvisionedBandwidth = 1 << 20
		req.stats.workCount = rand.Uint64()
		req.stats.writeAccountedBytes = rand.Uint64()
		req.stats.ingestedAccountedBytes = rand.Uint64()
		req.stats.statsToIgnore.ingestStats.Bytes = rand.Uint64()
		req.stats.statsToIgnore.ingestStats.ApproxIngestedIntoL0Bytes = rand.Uint64()
		req.stats.statsToIgnore.writeBytes = rand.Uint64()
	}
	kvGranter := &testGranterNonNegativeTokens{t: t}
	st := cluster.MakeTestingClusterSettings()
	ioll := ioLoadListener{
		settings:              st,
		kvRequester:           req,
		perWorkTokenEstimator: makeStorePerWorkTokenEstimator(),
		diskBandwidthLimiter:  newDiskBandwidthLimiter(),
		l0CompactedBytes:      metric.NewCounter(l0CompactedBytes),
		l0TokensProduced:      metric.NewCounter(l0TokensProduced),
	}
	ioll.kvGranter = kvGranter
	for i := 0; i < 100; i++ {
		randomValues()
		ioll.pebbleMetricsTick(ctx, StoreMetrics{
			Metrics:   &m,
			DiskStats: d,
		})
		for j := 0; j < int(loadedDuration.ticksInAdjustmentInterval()); j++ {
			ioll.allocateTokensTick(loadedDuration.ticksInAdjustmentInterval() - int64(i))
			require.LessOrEqual(t, int64(0), ioll.smoothedIntL0CompactedBytes)
			require.LessOrEqual(t, float64(0), ioll.smoothedCompactionByteTokens)
			require.LessOrEqual(t, float64(0), ioll.smoothedNumFlushTokens)
			require.LessOrEqual(t, float64(0), ioll.flushUtilTargetFraction)
			require.LessOrEqual(t, int64(0), ioll.totalNumByteTokens)
			require.LessOrEqual(t, int64(0), ioll.byteTokensAllocated)
			require.LessOrEqual(t, int64(0), ioll.diskWriteTokens)
			require.LessOrEqual(t, int64(0), ioll.diskWriteTokensAllocated)
			require.LessOrEqual(t, int64(0), ioll.diskReadTokens)
			require.LessOrEqual(t, int64(0), ioll.diskReadTokensAllocated)
		}
	}
}

type testRequesterForIOLL struct {
	stats storeAdmissionStats
	buf   strings.Builder
}

var _ storeRequester = &testRequesterForIOLL{}

func (r *testRequesterForIOLL) close() {}

func (r *testRequesterForIOLL) getRequesters() [admissionpb.NumWorkClasses]requester {
	panic("unimplemented")
}

func (r *testRequesterForIOLL) getStoreAdmissionStats() storeAdmissionStats {
	return r.stats
}

func (r *testRequesterForIOLL) setStoreRequestEstimates(estimates storeRequestEstimates) {
	fmt.Fprintf(&r.buf, "store-request-estimates: writeTokens: %d", estimates.writeTokens)
}

type testGranterWithIOTokens struct {
	buf                     strings.Builder
	allTokensUsed           bool
	diskBandwidthTokensUsed [admissionpb.NumStoreWorkTypes]diskTokens
}

var _ granterWithIOTokens = &testGranterWithIOTokens{}

func (g *testGranterWithIOTokens) setAvailableTokens(
	ioTokens int64,
	elasticIOTokens int64,
	elasticDiskBandwidthTokens int64,
	elasticReadBandwidthTokens int64,
	maxIOTokens int64,
	maxElasticIOTokens int64,
	maxElasticDiskBandwidthTokens int64,
	lastTick bool,
) (tokensUsed int64, tokensUsedByElasticWork int64) {
	fmt.Fprintf(&g.buf, "setAvailableTokens: io-tokens=%s(elastic %s) "+
		"elastic-disk-bw-tokens=%s read-bw-tokens=%s "+
		"max-byte-tokens=%s(elastic %s) max-disk-bw-tokens=%s lastTick=%t",
		tokensForTokenTickDurationToString(ioTokens),
		tokensForTokenTickDurationToString(elasticIOTokens),
		tokensForTokenTickDurationToString(elasticDiskBandwidthTokens),
		tokensForTokenTickDurationToString(elasticReadBandwidthTokens),
		tokensForTokenTickDurationToString(maxIOTokens),
		tokensForTokenTickDurationToString(maxElasticIOTokens),
		tokensForTokenTickDurationToString(maxElasticDiskBandwidthTokens),
		lastTick,
	)
	if g.allTokensUsed {
		return ioTokens * 2, 0
	}
	return 0, 0
}

func (g *testGranterWithIOTokens) getDiskTokensUsedAndReset() (
	usedTokens [admissionpb.NumStoreWorkTypes]diskTokens,
) {
	return g.diskBandwidthTokensUsed
}

func (g *testGranterWithIOTokens) setLinearModels(
	l0WriteLM tokensLinearModel,
	l0IngestLM tokensLinearModel,
	ingestLM tokensLinearModel,
	writeAmpLM tokensLinearModel,
) {
	fmt.Fprintf(&g.buf, "setAdmittedDoneModelsLocked: l0-write-lm: ")
	printLinearModel(&g.buf, l0WriteLM)
	fmt.Fprintf(&g.buf, " l0-ingest-lm: ")
	printLinearModel(&g.buf, l0IngestLM)
	fmt.Fprintf(&g.buf, " ingest-lm: ")
	printLinearModel(&g.buf, ingestLM)
	fmt.Fprintf(&g.buf, " write-amp-lm: ")
	printLinearModel(&g.buf, writeAmpLM)
	fmt.Fprintf(&g.buf, "\n")
}

func tokensForTokenTickDurationToString(tokens int64) string {
	if tokens >= unlimitedTokens/loadedDuration.ticksInAdjustmentInterval() {
		return "unlimited"
	}
	return fmt.Sprintf("%d", tokens)
}

type rawTokenResult adjustTokensResult

type testGranterNonNegativeTokens struct {
	t *testing.T
}

var _ granterWithIOTokens = &testGranterNonNegativeTokens{}

func (g *testGranterNonNegativeTokens) setAvailableTokens(
	ioTokens int64,
	elasticIOTokens int64,
	elasticDiskBandwidthTokens int64,
	elasticDiskReadBandwidthTokens int64,
	_ int64,
	_ int64,
	_ int64,
	_ bool,
) (tokensUsed int64, tokensUsedByElasticWork int64) {
	require.LessOrEqual(g.t, int64(0), ioTokens)
	require.LessOrEqual(g.t, int64(0), elasticIOTokens)
	require.LessOrEqual(g.t, int64(0), elasticDiskBandwidthTokens)
	require.LessOrEqual(g.t, int64(0), elasticDiskReadBandwidthTokens)
	return 0, 0
}

func (g *testGranterNonNegativeTokens) getDiskTokensUsedAndReset() (
	usedTokens [admissionpb.NumStoreWorkTypes]diskTokens,
) {
	return [admissionpb.NumStoreWorkTypes]diskTokens{}
}

func (g *testGranterNonNegativeTokens) setLinearModels(
	l0WriteLM tokensLinearModel,
	l0IngestLM tokensLinearModel,
	ingestLM tokensLinearModel,
	writeAmpLM tokensLinearModel,
) {
	require.LessOrEqual(g.t, 0.5, l0WriteLM.multiplier)
	require.LessOrEqual(g.t, int64(0), l0WriteLM.constant)
	require.Less(g.t, 0.0, l0IngestLM.multiplier)
	require.LessOrEqual(g.t, int64(0), l0IngestLM.constant)
	require.LessOrEqual(g.t, 0.5, ingestLM.multiplier)
	require.LessOrEqual(g.t, int64(0), ingestLM.constant)
	require.LessOrEqual(g.t, 1.0, writeAmpLM.multiplier)
	require.LessOrEqual(g.t, int64(0), writeAmpLM.constant)
}

// Tests if the tokenAllocationTicker produces correct adjustment interval
// durations for both loaded and unloaded systems.
func TestTokenAllocationTickerAdjustmentCalculation(t *testing.T) {
	// TODO(bananabrick): We might want to use a timeutil.TimeSource and
	// ManualTime for the tokenAllocationTicker, so that we can run this test
	// without any worry about flakes.
	skip.IgnoreLint(t)

	ticker := tokenAllocationTicker{}
	defer ticker.stop()
	currTime := timeutil.Now()
	ticker.adjustmentStart(true /* loaded */)
	adjustmentChanged := false
	for {
		<-ticker.ticker.C
		remainingTicks := ticker.remainingTicks()
		if remainingTicks == 0 {
			if adjustmentChanged {
				break
			}
			abs := func(diff time.Duration) time.Duration {
				if diff < 0 {
					return -diff
				}
				return diff
			}
			timeElapsed := timeutil.Since(currTime)
			diff := abs(timeElapsed - (15 * time.Second))
			if diff > 1*time.Second {
				t.FailNow()
			}
			ticker.adjustmentStart(false /* loaded */)
			currTime = timeutil.Now()
			adjustmentChanged = true
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func TestTokenAllocationTickerErrorAdjustmentThreshold(t *testing.T) {
	ticker := tokenAllocationTicker{}
	defer ticker.stop()
	ticker.adjustmentStart(false /* loaded */)

	// Knowing we are using unloaded duration. The first iteration will have 60 ticks.
	require.False(t, ticker.shouldAdjustForError(60 /* remainingTicks */, false /* loaded */))
	// Verify that we correctly reset the lastErrorAdjustmentTick value.
	require.Equal(t, uint64(60), ticker.lastErrorAdjustmentTick)

	// We should not do error adjustment unless 1s has passed. i.e. 4 ticks.
	require.False(t, ticker.shouldAdjustForError(58 /* remainingTicks */, false /* loaded */))
	require.True(t, ticker.shouldAdjustForError(56 /* remainingTicks */, false /* loaded */))
	require.Equal(t, uint64(56), ticker.lastErrorAdjustmentTick)

	// We should adjust for error on the last tick.
	require.True(t, ticker.shouldAdjustForError(0 /* remainingTicks */, false /* loaded */))

	// Re-run the above with loaded system. Now the error adjustment threshold is every 1000 ticks.
	require.False(t, ticker.shouldAdjustForError(15000 /* remainingTicks */, true /* loaded */))
	require.Equal(t, uint64(15000), ticker.lastErrorAdjustmentTick)
	require.False(t, ticker.shouldAdjustForError(14001 /* remainingTicks */, true /* loaded */))
	require.True(t, ticker.shouldAdjustForError(14000 /* remainingTicks */, true /* loaded */))
	require.Equal(t, uint64(14000), ticker.lastErrorAdjustmentTick)
	require.True(t, ticker.shouldAdjustForError(0 /* remainingTicks */, true /* loaded */))
}

func TestTokenAllocationTicker(t *testing.T) {
	// TODO(bananabrick): This might be flaky, in which case we should use a
	// timeutil.TimeSource and ManualTime in the tokenAllocationTicker for these
	// tests.
	ticker := tokenAllocationTicker{}
	defer ticker.stop()

	// Test remainingTicks calculations.
	ticker.adjustmentStart(false /* loaded */)
	require.Equal(t, 60, int(ticker.remainingTicks()))
	time.Sleep(1 * time.Second)
	// At least one second has passed, we assume that 2 seconds could've passed.
	// So, we have 13-14 seconds remaining.
	remaining := ticker.remainingTicks()
	if remaining < 52 || remaining > 56 {
		t.FailNow()
	}

	ticker.adjustmentStart(true /* unloaded */)
	require.Equal(t, 15000, int(ticker.remainingTicks()))
	time.Sleep(1 * time.Second)
	// At least one second has passed. Assume an error of at most one seconds, so
	// at most 2 seconds have passed. So, we have 13-14 seconds remaining.
	remaining = ticker.remainingTicks()
	if remaining > 14000 || remaining < 13000 {
		t.FailNow()
	}

	// Skip to the future in which case remainingTicks must be exhausted.
	ticker.adjustmentIntervalStartTime = timeutil.Now().Add(-17 * time.Second)
	require.Equal(t, 0, int(ticker.remainingTicks()))
}

func TestComputeCumStoreCompactionStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name       string
		baseLevel  int
		writeBytes int64
		sizeBytes  int64
		expected   cumStoreCompactionStats
	}{
		{
			name: "base-l6-zero",
			expected: cumStoreCompactionStats{
				numOutLevelsGauge: 1,
			},
		},
		{
			name:       "base-l6",
			baseLevel:  6,
			writeBytes: 50,
			sizeBytes:  500,
			expected: cumStoreCompactionStats{
				writeBytes:        50,
				numOutLevelsGauge: 1,
			},
		},
		{
			name:       "base-l2",
			baseLevel:  2,
			writeBytes: 97,
			sizeBytes:  397,
			expected: cumStoreCompactionStats{
				writeBytes:        97,
				numOutLevelsGauge: 5,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := pebble.Metrics{}
			var cumSizeBytes int64
			var cumWriteBytes uint64
			divisor := int64(len(m.Levels) - tc.baseLevel)
			for i := tc.baseLevel; i < len(m.Levels); i++ {
				m.Levels[i].Size = tc.sizeBytes / divisor
				cumSizeBytes += m.Levels[i].Size
				m.Levels[i].BytesCompacted = uint64(tc.writeBytes / divisor)
				cumWriteBytes += m.Levels[i].BytesCompacted
			}
			if cumSizeBytes < tc.sizeBytes {
				m.Levels[tc.baseLevel].Size += tc.sizeBytes - cumSizeBytes
			}
			if cumWriteBytes < uint64(tc.writeBytes) {
				m.Levels[tc.baseLevel].BytesCompacted += uint64(tc.writeBytes) - cumWriteBytes
			}
			require.Equal(t, tc.expected, computeCumStoreCompactionStats(&m))
		})
	}
}

func TestComputeL0CompactionTokensLowerBound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.Equal(t, int64(1000), computeL0CompactionTokensLowerBound(cumStoreCompactionStats{
		writeBytes:        3000,
		numOutLevelsGauge: 1,
	}, cumStoreCompactionStats{
		writeBytes:        8000,
		numOutLevelsGauge: 5,
	}))

	require.Equal(t, int64(0), computeL0CompactionTokensLowerBound(cumStoreCompactionStats{
		writeBytes:        1000,
		numOutLevelsGauge: 1,
	}, cumStoreCompactionStats{
		writeBytes:        500,
		numOutLevelsGauge: 2,
	}))
}
