// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	datadriven.RunTest(t, testutils.TestDataPath(t, "io_load_listener"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				ioll = &ioLoadListener{
					settings:              st,
					kvRequester:           req,
					perWorkTokenEstimator: makeStorePerWorkTokenEstimator(),
					diskBandwidthLimiter:  makeDiskBandwidthLimiter(),
				}
				// The mutex is needed by ioLoadListener but is not useful in this
				// test -- the channels provide synchronization and prevent this
				// test code and the ioLoadListener from being concurrently
				// active.
				ioll.mu.Mutex = &syncutil.Mutex{}
				ioll.mu.kvGranter = kvGranter
				return ""

			case "prep-admission-stats":
				req.stats = storeAdmissionStats{
					admittedCount:          0,
					writeAccountedBytes:    0,
					ingestedAccountedBytes: 0,
				}
				d.ScanArgs(t, "admitted", &req.stats.admittedCount)
				if d.HasArg("write-bytes") {
					d.ScanArgs(t, "write-bytes", &req.stats.writeAccountedBytes)
				}
				if d.HasArg("ingested-bytes") {
					d.ScanArgs(t, "ingested-bytes", &req.stats.ingestedAccountedBytes)
				}
				return fmt.Sprintf("%+v", req.stats)

			case "set-min-flush-util":
				var percent int
				d.ScanArgs(t, "percent", &percent)
				MinFlushUtilizationFraction.Override(ctx, &st.SV, float64(percent)/100)
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
				}
				flushMetric := pebble.ThroughputMetric{
					Bytes:        int64(flushBytes),
					WorkDuration: time.Duration(flushWorkSec) * time.Second,
					IdleDuration: time.Duration(flushIdleSec) * time.Second,
				}
				im := &pebble.InternalIntervalMetrics{}
				im.Flush.WriteThroughput = flushMetric
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
				if d.HasArg("disk-bw-tokens-used") {
					var regularTokensUsed, elasticTokensUsed int
					d.ScanArgs(t, "disk-bw-tokens-used", &regularTokensUsed, &elasticTokensUsed)
					kvGranter.diskBandwidthTokensUsed[regularWorkClass] = int64(regularTokensUsed)
					kvGranter.diskBandwidthTokensUsed[elasticWorkClass] = int64(elasticTokensUsed)
				} else {
					kvGranter.diskBandwidthTokensUsed[regularWorkClass] = 0
					kvGranter.diskBandwidthTokensUsed[elasticWorkClass] = 0
				}
				var printOnlyFirstTick bool
				if d.HasArg("print-only-first-tick") {
					d.ScanArgs(t, "print-only-first-tick", &printOnlyFirstTick)
				}
				ioll.pebbleMetricsTick(ctx, StoreMetrics{
					Metrics:                 &metrics,
					WriteStallCount:         int64(writeStallCount),
					InternalIntervalMetrics: im,
					DiskStats: DiskStats{
						BytesRead:            uint64(bytesRead),
						BytesWritten:         uint64(bytesWritten),
						ProvisionedBandwidth: int64(provisionedBandwidth),
					},
				})
				var buf strings.Builder
				// Do the ticks until just before next adjustment.
				res := ioll.adjustTokensResult
				fmt.Fprintln(&buf, redact.StringWithoutMarkers(&res))
				res.ioThreshold = nil // avoid nondeterminism
				fmt.Fprintf(&buf, "%+v\n", (rawTokenResult)(res))
				if req.buf.Len() > 0 {
					fmt.Fprintf(&buf, "%s\n", req.buf.String())
					req.buf.Reset()
				}
				for i := 0; i < ticksInAdjustmentInterval; i++ {
					ioll.allocateTokensTick()
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
		settings:    st,
		kvRequester: req,
	}
	ioll.mu.Mutex = &syncutil.Mutex{}
	ioll.mu.kvGranter = kvGranter
	// Bug 1: overflow when totalNumByteTokens is too large.
	for i := int64(0); i < adjustmentInterval; i++ {
		// Override the totalNumByteTokens manually to trigger the overflow bug.
		ioll.totalNumByteTokens = math.MaxInt64 - i
		ioll.byteTokensAllocated = 0
		for j := 0; j < ticksInAdjustmentInterval; j++ {
			ioll.allocateTokensTick()
		}
	}
	// Bug2: overflow when bytes added delta is 0.
	m := pebble.Metrics{}
	m.Levels[0] = pebble.LevelMetrics{
		Sublevels: 100,
		NumFiles:  10000,
	}
	ioll.pebbleMetricsTick(ctx,
		StoreMetrics{Metrics: &m, InternalIntervalMetrics: &pebble.InternalIntervalMetrics{}})
	ioll.pebbleMetricsTick(ctx,
		StoreMetrics{Metrics: &m, InternalIntervalMetrics: &pebble.InternalIntervalMetrics{}})
	ioll.allocateTokensTick()
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
		res := (*ioLoadListener)(nil).adjustTokensInner(
			ctx, tt.prev, tt.l0Metrics, 12, &pebble.InternalIntervalMetrics{},
			100, 10, 0.50)
		buf.Printf("%s\n", res)
	}
	echotest.Require(t, string(redact.Sprint(buf)), filepath.Join(testutils.TestDataPath(t, "format_adjust_tokens_stats.txt")))
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
		req.stats.admittedCount = rand.Uint64()
		req.stats.writeAccountedBytes = rand.Uint64()
		req.stats.ingestedAccountedBytes = rand.Uint64()
		req.stats.statsToIgnore.Bytes = rand.Uint64()
		req.stats.statsToIgnore.ApproxIngestedIntoL0Bytes = rand.Uint64()
	}
	kvGranter := &testGranterNonNegativeTokens{t: t}
	st := cluster.MakeTestingClusterSettings()
	ioll := ioLoadListener{
		settings:              st,
		kvRequester:           req,
		perWorkTokenEstimator: makeStorePerWorkTokenEstimator(),
		diskBandwidthLimiter:  makeDiskBandwidthLimiter(),
	}
	ioll.mu.Mutex = &syncutil.Mutex{}
	ioll.mu.kvGranter = kvGranter
	for i := 0; i < 100; i++ {
		randomValues()
		ioll.pebbleMetricsTick(ctx, StoreMetrics{
			Metrics:                 &m,
			InternalIntervalMetrics: &pebble.InternalIntervalMetrics{},
			DiskStats:               d,
		})
		for j := 0; j < ticksInAdjustmentInterval; j++ {
			ioll.allocateTokensTick()
			require.LessOrEqual(t, int64(0), ioll.smoothedIntL0CompactedBytes)
			require.LessOrEqual(t, float64(0), ioll.smoothedCompactionByteTokens)
			require.LessOrEqual(t, float64(0), ioll.smoothedNumFlushTokens)
			require.LessOrEqual(t, float64(0), ioll.flushUtilTargetFraction)
			require.LessOrEqual(t, int64(0), ioll.totalNumByteTokens)
			require.LessOrEqual(t, int64(0), ioll.byteTokensAllocated)
			require.LessOrEqual(t, int64(0), ioll.elasticDiskBWTokens)
			require.LessOrEqual(t, int64(0), ioll.elasticDiskBWTokensAllocated)
		}
	}
}

type testRequesterForIOLL struct {
	stats storeAdmissionStats
	buf   strings.Builder
}

var _ storeRequester = &testRequesterForIOLL{}

func (r *testRequesterForIOLL) close() {}

func (r *testRequesterForIOLL) getRequesters() [numWorkClasses]requester {
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
	diskBandwidthTokensUsed [numWorkClasses]int64
}

var _ granterWithIOTokens = &testGranterWithIOTokens{}

func (g *testGranterWithIOTokens) setAvailableIOTokensLocked(tokens int64) (tokensUsed int64) {
	fmt.Fprintf(&g.buf, "setAvailableIOTokens: %s", tokensForTokenTickDurationToString(tokens))
	if g.allTokensUsed {
		return tokens * 2
	}
	return 0
}

func (g *testGranterWithIOTokens) setAvailableElasticDiskBandwidthTokensLocked(tokens int64) {
	fmt.Fprintf(&g.buf, " setAvailableElasticDiskTokens: %s",
		tokensForTokenTickDurationToString(tokens))
}

func (g *testGranterWithIOTokens) getDiskTokensUsedAndResetLocked() [numWorkClasses]int64 {
	return g.diskBandwidthTokensUsed
}

func (g *testGranterWithIOTokens) setAdmittedDoneModelsLocked(
	l0WriteLM tokensLinearModel, l0IngestLM tokensLinearModel, ingestLM tokensLinearModel,
) {
	fmt.Fprintf(&g.buf, "setAdmittedDoneModelsLocked: l0-write-lm: ")
	printLinearModel(&g.buf, l0WriteLM)
	fmt.Fprintf(&g.buf, " l0-ingest-lm: ")
	printLinearModel(&g.buf, l0IngestLM)
	fmt.Fprintf(&g.buf, " ingest-lm: ")
	printLinearModel(&g.buf, ingestLM)
	fmt.Fprintf(&g.buf, "\n")
}

func tokensForTokenTickDurationToString(tokens int64) string {
	if tokens >= unlimitedTokens/ticksInAdjustmentInterval {
		return "unlimited"
	}
	return fmt.Sprintf("%d", tokens)
}

type rawTokenResult adjustTokensResult

type testGranterNonNegativeTokens struct {
	t *testing.T
}

var _ granterWithIOTokens = &testGranterNonNegativeTokens{}

func (g *testGranterNonNegativeTokens) setAvailableIOTokensLocked(tokens int64) (tokensUsed int64) {
	require.LessOrEqual(g.t, int64(0), tokens)
	return 0
}

func (g *testGranterNonNegativeTokens) setAvailableElasticDiskBandwidthTokensLocked(tokens int64) {
	require.LessOrEqual(g.t, int64(0), tokens)
}

func (g *testGranterNonNegativeTokens) getDiskTokensUsedAndResetLocked() [numWorkClasses]int64 {
	return [numWorkClasses]int64{}
}

func (g *testGranterNonNegativeTokens) setAdmittedDoneModelsLocked(
	l0WriteLM tokensLinearModel, l0IngestLM tokensLinearModel, ingestLM tokensLinearModel,
) {
	require.LessOrEqual(g.t, 0.5, l0WriteLM.multiplier)
	require.LessOrEqual(g.t, int64(0), l0WriteLM.constant)
	require.Less(g.t, 0.0, l0IngestLM.multiplier)
	require.LessOrEqual(g.t, int64(0), l0IngestLM.constant)
	require.LessOrEqual(g.t, 0.5, ingestLM.multiplier)
	require.LessOrEqual(g.t, int64(0), ingestLM.constant)
}
