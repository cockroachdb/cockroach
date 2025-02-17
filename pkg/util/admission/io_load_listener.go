// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
)

// MinFlushUtilizationFraction is a lower-bound on the dynamically adjusted
// flush utilization target fraction that attempts to reduce write stalls. Set
// it to a high fraction (>>1, e.g. 10), to effectively disable flush based
// tokens.
//
// The target fraction is used to multiply the (measured) peak flush rate, to
// compute the flush tokens. For example, if the dynamic target fraction (for
// which this setting provides a lower bound) is currently 0.75, then
// 0.75*peak-flush-rate will be used to set the flush tokens. The lower bound
// of 0.5 should not need to be tuned, and should not be tuned without
// consultation with a domain expert. If the storage.write-stall-nanos
// indicates significant write stalls, and the granter logs show that the
// dynamic target fraction has already reached the lower bound, one can
// consider lowering it slightly and then observe the effect.
var MinFlushUtilizationFraction = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.min_flush_util_fraction",
	"when computing flush tokens, this fraction is a lower bound on the dynamically "+
		"adjusted flush utilization target fraction that attempts to reduce write stalls. Set "+
		"it to a high fraction (>>1, e.g. 10), to disable flush based tokens. The dynamic "+
		"target fraction is used to multiply the (measured) peak flush rate, to compute the flush "+
		"tokens. If the storage.write-stall-nanos indicates significant write stalls, and the granter "+
		"logs show that the dynamic target fraction has already reached the lower bound, one can "+
		"consider lowering it slightly (after consultation with domain experts)", 0.5,
	settings.PositiveFloat)

// DiskBandwidthTokensForElasticEnabled controls whether the disk bandwidth
// resource is considered as a possible bottleneck resource. When it becomes a
// bottleneck, tokens for elastic work are limited based on available disk
// bandwidth. The default is true since actually considering disk bandwidth as
// a bottleneck resource requires additional configuration (outside the
// admission package) to calculate the provisioned bandwidth.
var DiskBandwidthTokensForElasticEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.disk_bandwidth_tokens.elastic.enabled",
	"when true, and provisioned bandwidth for the disk corresponding to a store is configured, "+
		"tokens for elastic work will be limited if disk bandwidth becomes a bottleneck",
	true,
	settings.WithPublic)

// L0FileCountOverloadThreshold sets a file count threshold that signals an
// overloaded store.
var L0FileCountOverloadThreshold = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"admission.l0_file_count_overload_threshold",
	"when the L0 file count exceeds this theshold, the store is considered overloaded",
	l0FileCountOverloadThreshold, settings.PositiveInt)

// L0SubLevelCountOverloadThreshold sets a sub-level count threshold that
// signals an overloaded store.
var L0SubLevelCountOverloadThreshold = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"admission.l0_sub_level_count_overload_threshold",
	"when the L0 sub-level count exceeds this threshold, the store is considered overloaded",
	l0SubLevelCountOverloadThreshold, settings.PositiveInt)

// ElasticBandwidthMaxUtil sets the max utilization for disk bandwidth for elastic traffic.
var ElasticBandwidthMaxUtil = settings.RegisterFloatSetting(
	settings.SystemOnly, "kvadmission.store.elastic_disk_bandwidth_max_util",
	"sets the max utilization for disk bandwidth for elastic traffic",
	0.8,
	settings.NonNegativeFloatWithMaximum(1.0),
	settings.FloatWithMinimum(0.05),
)

// L0MinimumSizePerSubLevel is a minimum size threshold per sub-level, to
// avoid over reliance on the sub-level count as a signal of overload. Pebble
// sometimes has to do frequent flushes of the memtable due to ingesting
// sstables that overlap with the memtable, and each flush may generate a
// sub-level. We have seen situations where these flushes have a tiny amount
// of bytes, but a sequence of these can result in a high sub-level count.
// The default of 5MB is chosen since:
// 5MB*l0SubLevelCountOverloadThreshold=100MB, which can be very quickly
// compacted into Lbase (say Lbase overlapping bytes are 200MB, this is a
// 100MB+200MB=300MB compaction, which takes < 15s).
//
// NB: 5MB is typically significantly smaller than the flush size of a 64MB
// memtable (after accounting for compression when flushing the memtable). If
// it were comparable, this size based computation of sub-levels would
// typically override the actual sub-levels, which would defeat the point of
// using the sub-level count as a metric to guide admission.
//
// Setting this to 0 disables this minimum size logic.
var L0MinimumSizePerSubLevel = settings.RegisterIntSetting(
	settings.SystemOnly,
	"admission.l0_min_size_per_sub_level",
	"when non-zero, this indicates the minimum size that is needed to count towards one sub-level",
	5<<20, settings.NonNegativeInt)

// This flag was introduced before any experience with WAL failover. It turns
// out that WAL failover can sometimes be triggered because the disk is
// overloaded, and allowing unlimited tokens is going to make it worse. So do
// not set this to true without consulting with an expert.
var walFailoverUnlimitedTokens = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.wal.failover.unlimited_tokens.enabled",
	"when true, during WAL failover, unlimited admission tokens are allocated",
	false)

// The following experimental observations were used to guide the initial
// implementation, which aimed to maintain a sub-level count of 20 with token
// calculation every 60s. Since then, the code has evolved to calculate tokens
// every 15s and to aim for regular work maintaining a sub-level count of
// l0SubLevelCountOverloadThreshold/2. So this commentary should be
// interpreted in that context:
//   - Sub-level count of ~40 caused a node heartbeat latency p90, p99 of 2.5s,
//     4s. With a setting that limits sub-level count to 10, before the system
//     is considered overloaded, and adjustmentInterval = 60, we see the actual
//     sub-level count ranging from 5-30, with p90, p99 node heartbeat latency
//     showing a similar wide range, with 1s, 2s being the middle of the range
//     respectively.
//   - With tpcc, we sometimes see a sub-level count > 10 with only 100 files in
//     L0. We don't want to restrict tokens in this case since the store is able
//     to recover on its own. One possibility would be to require both the
//     thresholds to be exceeded before we consider the store overloaded. But
//     then we run the risk of having 100+ sub-levels when we hit a file count
//     of 1000. Instead we use a sub-level overload threshold of 20.
//
// A sub-level count of l0SubLevelCountOverloadThreshold results in the same
// score as a file count of l0FileCountOverloadThreshold. Exceptions: a small
// L0 in terms of bytes (see IOThreshold.Score); these constants being
// overridden in the cluster settings
// admission.l0_sub_level_count_overload_threshold and
// admission.l0_file_count_overload_threshold. We ignore these exceptions in
// the discussion here. Hence, 20 sub-levels is equivalent in score to 4000 L0
// files, i.e., 1 sub-level is equivalent to 200 files.
//
// Ideally, equivalence here should match equivalence in how L0 is scored for
// compactions. CockroachDB sets Pebble's L0CompactionThreshold to a constant
// value of 2, which results in a compaction score of 1.0 with 1 sub-level.
// CockroachDB does not override Pebble's L0CompactionFileThreshold, which
// defaults to 500, so 500 files cause a compaction score of 1.0. So in
// Pebble's compaction scoring logic, 1 sub-level is equivalent to 500 L0
// files.
//
// So admission control is more sensitive to higher file count than Pebble's
// compaction scoring. l0FileCountOverloadThreshold used to be 1000 up to
// v24.3, and increasing it to 4000 was considered significant enough --
// increasing to 10000, to make Pebble's compaction logic and admission
// control equivalent was considered too risky. Note that admission control
// tries to maintain a score of 0.5 when admitting regular work, which if
// caused by file count represent 2000 files. With 2000 files, the L0
// compaction score is 2000/500 = 4.0, which is significantly above the
// compaction threshold of 1.0 (at which a level is eligible for compaction).
// So one could argue that this inconsistency between admission control and
// Pebble is potentially harmless.
const l0FileCountOverloadThreshold = 4000
const l0SubLevelCountOverloadThreshold = 20

// ioLoadListener adjusts tokens in kvStoreTokenGranter for IO, specifically due to
// overload caused by writes. IO uses tokens and not slots since work
// completion is not an indicator that the "resource usage" has ceased -- it
// just means that the write has been applied to the WAL. Most of the work is
// in flushing to sstables and the following compactions, which happens later.
//
// Token units are in bytes and used to protect a number of virtual or
// physical resource bottlenecks:
//   - Compactions out of L0: compactions out of L0 can fall behind and cause
//     too many sub-levels or files in L0.
//   - Flushes into L0: flushes of memtables to L0 can fall behind and cause
//     write stalls due to too many memtables.
//   - Disk bandwidth: there is typically an aggregate read+write provisioned
//     bandwidth, and if it is fully utilized, IO operations can start queueing
//     and encounter high latency.
//
// For simplicity, after ioLoadListener computes the tokens due to compaction
// or flush bottlenecks, it computes the minimum and passes that value to
// granterWithIOTokens.setAvailableIOTokensLocked. That is, instead of working
// with multiple token dimensions, these two token dimensions get collapsed
// into one for enforcement purposes. This also helps simplify the integration
// with WorkQueue which is dealing with a single dimension. The consumption of
// these tokens is based on how many bytes an admitted work adds to L0.
//
// The disk bandwidth constraint is used to compute a token count for elastic
// work (see disk_bandwidth.go for the reasons why this is limited to elastic
// work). Additionally, these tokens are meant be consumed for all incoming
// bytes into the LSM, and not just those written to L0 e.g. ingested bytes
// into L3 should also consume such tokens. Note that we call these disk
// bandwidth tokens, but that is a misnomer -- these are tokens for incoming
// bytes into the LSM, motivated by disk bandwidth as a bottleneck resource,
// and not consumed for every write to the disk (e.g. by compactions). Since
// these tokens are consumed by all incoming bytes into the LSM, and not just
// those into L0, it suggests explicitly modeling this as a separate
// dimension. However, since modeling as a separate dimension everywhere would
// lead to code complexity, we adopt the following compromise:
//
//   - Like the other token dimensions, ioLoadListener computes a different
//     elastic token count (using diskBandwidthLimiter), and a different model
//     for consumption (via
//     storePerWorkTokenEstimator.atDoneDiskBWTokensLinearModel).
//
//   - granterWithIOTokens, implemented by kvStoreTokenGranter, which enforces
//     the token count, also treats this as a separate dimension.
//
//   - WorkQueue works with a single dimension, so the tokens consumed at
//     admission time are based on L0-bytes estimate. However, when
//     StoreWorkQueue informs kvStoreTokenGranter of work completion (by calling
//     storeWriteDone), the tokens are adjusted differently for the
//     flush/compaction L0 tokens and for the "disk bandwidth" tokens.
type ioLoadListener struct {
	storeID     roachpb.StoreID
	settings    *cluster.Settings
	kvRequester storeRequester
	kvGranter   granterWithIOTokens

	// Stats used to compute interval stats.
	statsInitialized bool
	adjustTokensResult
	perWorkTokenEstimator storePerWorkTokenEstimator
	diskBandwidthLimiter  *diskBandwidthLimiter

	l0CompactedBytes *metric.Counter
	l0TokensProduced *metric.Counter
}

type ioLoadListenerState struct {
	// Cumulative.
	cumL0AddedBytes uint64
	// Gauge.
	curL0Bytes int64
	// Cumulative.
	cumWriteStallCount      int64
	cumFlushWriteThroughput pebble.ThroughputMetric
	diskBW                  struct {
		// Cumulative
		bytesRead    uint64
		bytesWritten uint64
	}
	cumCompactionStats           cumStoreCompactionStats
	cumWALSecondaryWriteDuration time.Duration
	unflushedMemTableTooLarge    bool

	// Exponentially smoothed per interval values.

	smoothedIntL0CompactedBytes int64 // bytes leaving L0
	// Smoothed history of byte tokens calculated based on compactions out of L0.
	smoothedCompactionByteTokens float64

	// Smoothed history of flush tokens calculated based on memtable flushes,
	// before multiplying by target fraction.
	smoothedNumFlushTokens float64
	// The target fraction to be used for the effective flush tokens. It is in
	// the interval
	// [MinFlushUtilizationFraction,maxFlushUtilTargetFraction].
	flushUtilTargetFraction float64

	// totalNumByteTokens represents the tokens to give out until the next call to
	// adjustTokens. They are parceled out in small intervals. byteTokensAllocated
	// represents what has been given out.
	totalNumByteTokens  int64
	byteTokensAllocated int64
	// Used tokens can be negative if some tokens taken in one interval were
	// returned in another, but that will be extremely rare.
	byteTokensUsed              int64
	byteTokensUsedByElasticWork int64

	totalNumElasticByteTokens  int64
	elasticByteTokensAllocated int64

	// diskWriteTokens represents the tokens to give out until the next
	// call to adjustTokens. They are parceled out in small intervals.
	// diskWriteTokensAllocated represents what has been given out.
	diskWriteTokens          int64
	diskWriteTokensAllocated int64
	// diskReadTokens are tokens that were already deducted during token
	// estimation. These tokens are used for read error accounting
	// adjustDiskTokenErrorLocked.
	diskReadTokens          int64
	diskReadTokensAllocated int64
}

type cumStoreCompactionStats struct {
	writeBytes uint64
	// Not cumulative. This is the number of levels from which bytes will be
	// moved out to lower levels via compactions.
	numOutLevelsGauge int
}

func computeCumStoreCompactionStats(m *pebble.Metrics) cumStoreCompactionStats {
	var compactedWriteBytes uint64
	baseLevel := -1
	for i := range m.Levels {
		compactedWriteBytes += m.Levels[i].BytesCompacted
		if i > 0 && m.Levels[i].Size > 0 && baseLevel < 0 {
			baseLevel = i
		}
	}
	if baseLevel < 0 {
		baseLevel = len(m.Levels) - 1
	}
	return cumStoreCompactionStats{
		writeBytes:        compactedWriteBytes,
		numOutLevelsGauge: len(m.Levels) - baseLevel,
	}
}

// computeL0CompactionTokensLowerBound is used to give some tokens to L0 even
// if compactions out of L0 are not happening because the compaction score is
// not high enough. This allows admission until compaction score is high
// enough, at which point we stop using l0CompactionTokensLowerBound.
//
// See https://github.com/cockroachdb/cockroach/issues/115373 for motivation.
func computeL0CompactionTokensLowerBound(
	prev cumStoreCompactionStats, cur cumStoreCompactionStats,
) int64 {
	// All levels are expected to have similar write amp, so we expect that each
	// will be given equal compaction resources. This isn't actually true if the
	// levels have very different scores, but it suffices for the purpose of
	// this lower bound. Consider an actual incident where L0 was not
	// being compacted because it only had 5 sub-levels, and
	// numOutLevelsGauge=6, and the aggregate compaction bandwidth was ~140MB/s
	// distributed over 7 concurrent compactions. The aggregated compacted in 15
	// seconds is 2100MB. So we return 2100MB/6 = 350MB of tokens.
	intCompactedWriteBytes := int64(cur.writeBytes) - int64(prev.writeBytes)
	if intCompactedWriteBytes < 0 {
		// Defensive: ignore bogus stats.
		intCompactedWriteBytes = 0
	}
	return intCompactedWriteBytes / int64(cur.numOutLevelsGauge)
}

const unlimitedTokens = math.MaxInt64

// Token changes are made at a coarse time granularity of 15s since
// compactions can take ~10s to complete. The totalNumByteTokens to give out over
// the 15s interval are given out in a smoothed manner, at either 1ms intervals,
// or 250ms intervals depending on system load.
// This has similarities with the following kinds of token buckets:
//   - Zero replenishment rate and a burst value that is changed every 15s. We
//     explicitly don't want a huge burst every 15s.
//   - For loaded systems, a replenishment rate equal to
//     totalNumByteTokens/15000(once per ms), with a burst capped at
//     totalNumByteTokens/60.
//   - For unloaded systems, a replenishment rate equal to
//     totalNumByteTokens/60(once per 250ms), with a burst capped at
//     totalNumByteTokens/60.
//   - The only difference with the code here is that if totalNumByteTokens is
//     small, the integer rounding effects are compensated for.
//
// In an experiment with extreme overload using KV0 with block size 64KB,
// and 4096 clients, we observed the following states of L0 at 1min
// intervals (r-amp is the L0 sub-level count), in the absence of any
// admission control:
//
// __level_____count____size___score______in__ingest(sz_cnt)____move(sz_cnt)___write(sz_cnt)____read___r-amp___w-amp›
//
//	0        96   158 M    2.09   315 M     0 B       0     0 B       0   305 M     178     0 B       3     1.0›
//	0      1026   1.7 G    3.15   4.7 G     0 B       0     0 B       0   4.7 G   2.8 K     0 B      24     1.0›
//	0      1865   3.0 G    2.86   9.1 G     0 B       0     0 B       0   9.1 G   5.5 K     0 B      38     1.0›
//	0      3225   4.9 G    3.46    13 G     0 B       0     0 B       0    13 G   8.3 K     0 B      59     1.0›
//	0      4720   7.0 G    3.46    17 G     0 B       0     0 B       0    17 G    11 K     0 B      85     1.0›
//	0      6120   9.0 G    4.13    21 G     0 B       0     0 B       0    21 G    14 K     0 B     109     1.0›
//
// Note the fast growth in sub-level count. Production issues typically have
// slower growth towards an unhealthy state (remember that similar stats in
// the logs of a regular CockroachDB node are at 10min intervals, and not at
// 1min).
//
// In the above experiment, L0 compaction durations at 200+ sub-levels were
// usually sane, with most L0 compactions < 10s, and with a bandwidth of
// ~80MB/s. There were some 1-2GB compactions that took ~20s. The
// expectation is that with the throttling done by admission control here,
// we should almost never see multi-minute compactions. Which makes it
// reasonable to simply use metrics that are updated when compactions
// complete (as opposed to also tracking progress in bytes of on-going
// compactions).
//
// An interval < 250ms is picked to hand out the computed tokens due to the needs
// of flush tokens. For compaction tokens, a 1s interval is fine-grained enough.
//
// If flushing a memtable takes 100ms, then 10 memtables can be sustainably
// flushed in 1s. If we dole out flush tokens in 1s intervals, then there are
// enough tokens to create 10 memtables at the very start of a 1s interval,
// which will cause a write stall. Intuitively, the faster it is to flush a
// memtable, the smaller the interval for doling out these tokens. We have
// observed flushes taking ~0.5s, so we need to pick an interval less than 0.5s,
// say 250ms, for doling out these tokens.
//
// We use a 1ms interval for handing out tokens, to avoid upto 250ms wait times
// for high priority requests. As a simple example, consider a scenario where
// each request needs 1 byte token, and there are 1000 tokens added every 250ms.
// There is a uniform arrival rate of 2000 high priority requests/s, so 500
// requests uniformly distributed over 250ms. And a uniform arrival rate of
// 10,000/s of low priority requests, so 2500 requests uniformly distributed over
// 250ms. There are more than enough tokens to fully satisfy the high priority
// tokens (they use only 50% of the tokens), but not enough for the low priority
// requests. Ignore the fact that the latter will result in indefinite queue
// growth in the admission control WorkQueue. At a particular 250ms tick, the
// token bucket will go from 0 tokens to 1000 tokens. Any queued high priority
// requests will be immediately granted their token, until there are no queued
// high priority requests. Then since there are always a large number of low
// priority requests waiting, they will be granted until 0 tokens remain. Now we
// have a 250ms duration until the next replenishment and 0 tokens, so any high
// priority requests arriving will have to wait. The maximum wait time is 250ms.
//
// We use a 250ms intervals for underloaded systems, to avoid CPU utilization
// issues (see the discussion in runnable.go).
const adjustmentInterval = 15

type tickDuration time.Duration

func (t tickDuration) ticksInAdjustmentInterval() int64 {
	return 15 * int64(time.Second/time.Duration(t))
}

const unloadedDuration = tickDuration(250 * time.Millisecond)
const loadedDuration = tickDuration(1 * time.Millisecond)

// TODO(aaditya): Consider lowering this threshold. It was picked arbitrarily
// and seems to work well enough. Would it be better to do error accounting at
// an even higher frequency?
const errorAdjustmentInterval = 1
const errorTicksInAdjustmentInterval = int64(adjustmentInterval / errorAdjustmentInterval)

// tokenAllocationTicker wraps a time.Ticker, and also computes the remaining
// ticks in the adjustment interval, given an expected tick rate. If every tick
// from the ticker was always equal to the expected tick rate, then we could
// easily determine the remaining ticks, but each tick of time.Ticker can have
// drift, especially for tiny tick rates like 1ms.
type tokenAllocationTicker struct {
	expectedTickDuration        time.Duration
	adjustmentIntervalStartTime time.Time
	lastErrorAdjustmentTick     uint64
	ticker                      *time.Ticker
}

// Start a new adjustment interval. adjustmentStart must be called before tick
// is called. After the initial call, adjustmentStart must also be called if
// remainingticks returns 0, to indicate that a new adjustment interval has
// started.
func (t *tokenAllocationTicker) adjustmentStart(loaded bool) {
	// For each adjustmentInterval, we pick a tick rate depending on the system
	// load. If the system is unloaded, we tick at a 250ms rate, and if the system
	// is loaded, we tick at a 1ms rate. See the comment above the
	// adjustmentInterval definition to see why we tick at different rates.
	tickDur := unloadedDuration
	if loaded {
		tickDur = loadedDuration
	}
	t.expectedTickDuration = time.Duration(tickDur)
	if t.ticker == nil {
		t.ticker = time.NewTicker(t.expectedTickDuration)
	} else {
		t.ticker.Reset(t.expectedTickDuration)
	}
	t.adjustmentIntervalStartTime = timeutil.Now()
}

// remainingTicks will return the remaining ticks before the next adjustment
// interval is reached while assuming that all future ticks will have a duration of
// expectedTickDuration. A return value of 0 indicates that adjustmentStart must
// be called, as the previous adjustmentInterval is over.
func (t *tokenAllocationTicker) remainingTicks() uint64 {
	timePassed := timeutil.Since(t.adjustmentIntervalStartTime)
	if timePassed > adjustmentInterval*time.Second {
		return 0
	}
	remainingTime := adjustmentInterval*time.Second - timePassed
	return uint64((remainingTime + t.expectedTickDuration - 1) / t.expectedTickDuration)
}

// shouldAdjustForError returns true if we should additionally adjust for read
// and write error based on the number of remainingTicks and
// errorAdjustmentInterval.
func (t *tokenAllocationTicker) shouldAdjustForError(remainingTicks uint64, loaded bool) bool {
	tickDur := unloadedDuration
	if loaded {
		tickDur = loadedDuration
	}
	if t.lastErrorAdjustmentTick == 0 {
		// If this is the first tick of a new adjustment period, reset the 0 value
		// to the total number of ticks (equivalent values for the purpose of error
		// accounting).
		t.lastErrorAdjustmentTick = uint64(tickDur.ticksInAdjustmentInterval())
	}
	// We calculate the number of ticks in the errorAdjustmentDuration.
	errorTickThreshold := uint64(tickDur.ticksInAdjustmentInterval() / errorTicksInAdjustmentInterval)
	// We adjust for error when either we have passed the errorAdjustmentInterval
	// threshold or it is the last tick before the new adjustment interval.
	shouldAdjust := t.lastErrorAdjustmentTick-remainingTicks >= errorTickThreshold || remainingTicks == 0
	if !shouldAdjust {
		return false
	}
	// Since lastErrorAdjustmentTick uses the remainingTicks in the previous
	// iteration, it is expected to be a decreasing value over time. The expected
	// range is [ticksInAdjustmentInterval, 0].
	t.lastErrorAdjustmentTick = remainingTicks
	return true
}

func (t *tokenAllocationTicker) stop() {
	t.ticker.Stop()
	*t = tokenAllocationTicker{}
}

func cumLSMIngestedBytes(m *pebble.Metrics) (ingestedBytes uint64) {
	for i := range m.Levels {
		ingestedBytes += m.Levels[i].BytesIngested
	}
	return ingestedBytes
}

func replaceFlushThroughputBytesBySSTableWriteThroughput(m *pebble.Metrics) {
	m.Flush.WriteThroughput.Bytes = int64(m.Levels[0].BytesFlushed)
}

// pebbleMetricsTicks is called every adjustmentInterval seconds, and decides
// the token allocations until the next call. Returns true iff the system is
// loaded.
func (io *ioLoadListener) pebbleMetricsTick(ctx context.Context, metrics StoreMetrics) bool {
	ctx = logtags.AddTag(ctx, "s", io.storeID)
	m := metrics.Metrics
	replaceFlushThroughputBytesBySSTableWriteThroughput(m)
	if !io.statsInitialized {
		io.statsInitialized = true
		sas := io.kvRequester.getStoreAdmissionStats()
		cumIngestBytes := cumLSMIngestedBytes(metrics.Metrics)
		io.perWorkTokenEstimator.updateEstimates(
			metrics.Levels[0], cumIngestBytes, metrics.DiskStats.BytesWritten, sas, false)
		io.adjustTokensResult = adjustTokensResult{
			ioLoadListenerState: ioLoadListenerState{
				cumL0AddedBytes:              m.Levels[0].BytesFlushed + m.Levels[0].BytesIngested,
				curL0Bytes:                   m.Levels[0].Size,
				cumWriteStallCount:           metrics.WriteStallCount,
				cumFlushWriteThroughput:      m.Flush.WriteThroughput,
				cumCompactionStats:           computeCumStoreCompactionStats(m),
				cumWALSecondaryWriteDuration: m.WAL.Failover.SecondaryWriteDuration,
				// No initial limit, i.e, the first interval is unlimited.
				totalNumByteTokens:        unlimitedTokens,
				totalNumElasticByteTokens: unlimitedTokens,
				diskWriteTokens:           unlimitedTokens,
				// Currently, disk read tokens are only used to assess how many tokens
				// were deducted from the writes bucket to account for future reads. A 0
				// value here represents that.
				diskReadTokens: 0,
			},
			aux: adjustTokensAuxComputations{},
			ioThreshold: &admissionpb.IOThreshold{
				L0NumSubLevels:           int64(m.Levels[0].Sublevels),
				L0NumSubLevelsThreshold:  math.MaxInt64,
				L0NumFiles:               m.Levels[0].NumFiles,
				L0NumFilesThreshold:      math.MaxInt64,
				L0Size:                   m.Levels[0].Size,
				L0MinimumSizePerSubLevel: 0,
			},
		}
		io.diskBW.bytesRead = metrics.DiskStats.BytesRead
		io.diskBW.bytesWritten = metrics.DiskStats.BytesWritten
		io.copyAuxEtcFromPerWorkEstimator()

		// Assume system starts off unloaded.
		return false
	}
	io.adjustTokens(ctx, metrics)
	io.cumFlushWriteThroughput = metrics.Flush.WriteThroughput
	// We assume that the system is loaded if there is less than unlimited tokens
	// available.
	return io.totalNumByteTokens < unlimitedTokens || io.totalNumElasticByteTokens < unlimitedTokens
}

// For both byte and disk bandwidth tokens, allocateTokensTick gives out
// remainingTokens/remainingTicks tokens in the current tick.
func (io *ioLoadListener) allocateTokensTick(remainingTicks int64) {
	allocateFunc := func(total int64, allocated int64, remainingTicks int64) (toAllocate int64) {
		remainingTokens := total - allocated
		// remainingTokens can be equal to unlimitedTokens(MaxInt64) if allocated ==
		// 0. In such cases remainingTokens + remainingTicks - 1 will overflow.
		if remainingTokens >= unlimitedTokens-(remainingTicks-1) {
			toAllocate = remainingTokens / remainingTicks
		} else {
			// Round up so that we don't accumulate tokens to give in a burst on
			// the last tick.
			//
			// TODO(bananabrick): Rounding up is a problem for 1ms tick rate as we tick
			// up to 15000 times. Say totalNumByteTokens is 150001. We round up to give
			// 11 tokens per ms. So, we'll end up distributing the 150001 available
			// tokens in 150000/11 == 13637 remainingTicks. So, we'll have over a
			// second where we grant no tokens. Larger values of totalNumBytesTokens
			// will ease this problem.
			toAllocate = (remainingTokens + remainingTicks - 1) / remainingTicks
			if toAllocate < 0 {
				panic(errors.AssertionFailedf("toAllocate is negative %d", toAllocate))
			}
			if toAllocate+allocated > total {
				toAllocate = total - allocated
			}
		}
		return toAllocate
	}
	// INVARIANT: toAllocate* >= 0.
	toAllocateByteTokens := allocateFunc(
		io.totalNumByteTokens,
		io.byteTokensAllocated,
		remainingTicks,
	)
	if toAllocateByteTokens < 0 {
		panic(errors.AssertionFailedf("toAllocateByteTokens is negative %d", toAllocateByteTokens))
	}
	toAllocateElasticByteTokens := allocateFunc(
		io.totalNumElasticByteTokens, io.elasticByteTokensAllocated, remainingTicks)
	if toAllocateElasticByteTokens < 0 {
		panic(errors.AssertionFailedf("toAllocateElasticByteTokens is negative %d",
			toAllocateElasticByteTokens))
	}
	toAllocateDiskWriteTokens :=
		allocateFunc(
			io.diskWriteTokens,
			io.diskWriteTokensAllocated,
			remainingTicks,
		)
	if toAllocateDiskWriteTokens < 0 {
		panic(errors.AssertionFailedf("toAllocateDiskWriteTokens is negative %d",
			toAllocateDiskWriteTokens))
	}
	io.diskWriteTokensAllocated += toAllocateDiskWriteTokens

	toAllocateDiskReadTokens :=
		allocateFunc(
			io.diskReadTokens,
			io.diskReadTokensAllocated,
			remainingTicks,
		)
	if toAllocateDiskReadTokens < 0 {
		panic(errors.AssertionFailedf("toAllocateDiskReadTokens is negative %d",
			toAllocateDiskReadTokens))
	}
	io.diskReadTokensAllocated += toAllocateDiskReadTokens

	// INVARIANT: toAllocate >= 0.
	io.byteTokensAllocated += toAllocateByteTokens
	if io.byteTokensAllocated < 0 {
		panic(errors.AssertionFailedf("tokens allocated is negative %d", io.byteTokensAllocated))
	}
	io.elasticByteTokensAllocated += toAllocateElasticByteTokens
	if io.elasticByteTokensAllocated < 0 {
		panic(errors.AssertionFailedf(
			"tokens allocated is negative %d", io.elasticByteTokensAllocated))
	}

	tokensMaxCapacity := allocateFunc(
		io.totalNumByteTokens, 0, unloadedDuration.ticksInAdjustmentInterval(),
	)
	elasticTokensMaxCapacity := allocateFunc(
		io.totalNumElasticByteTokens, 0, unloadedDuration.ticksInAdjustmentInterval(),
	)
	diskWriteTokenMaxCapacity := allocateFunc(
		io.diskWriteTokens, 0, unloadedDuration.ticksInAdjustmentInterval(),
	)

	tokensUsed, tokensUsedByElasticWork := io.kvGranter.setAvailableTokens(
		toAllocateByteTokens,
		toAllocateElasticByteTokens,
		toAllocateDiskWriteTokens,
		toAllocateDiskReadTokens,
		tokensMaxCapacity,
		elasticTokensMaxCapacity,
		diskWriteTokenMaxCapacity,
		remainingTicks == 1,
	)
	io.byteTokensUsed += tokensUsed
	io.byteTokensUsedByElasticWork += tokensUsedByElasticWork
}

func computeIntervalDiskLoadInfo(
	prevCumBytesRead uint64, prevCumBytesWritten uint64, diskStats DiskStats, elasticBWUtil float64,
) intervalDiskLoadInfo {
	return intervalDiskLoadInfo{
		intReadBytes:            int64(diskStats.BytesRead - prevCumBytesRead),
		intWriteBytes:           int64(diskStats.BytesWritten - prevCumBytesWritten),
		intProvisionedDiskBytes: diskStats.ProvisionedBandwidth * adjustmentInterval,
		elasticBandwidthMaxUtil: elasticBWUtil,
	}
}

// adjustTokens computes a new value of totalNumByteTokens (and resets
// tokensAllocated). The new value, when overloaded, is based on comparing how
// many bytes are being moved out of L0 via compactions with the average
// number of bytes being added to L0 per KV work. We want the former to be
// (significantly) larger so that L0 returns to a healthy state. The byte
// token computation also takes into account the flush throughput, since an
// inability to flush fast enough can result in write stalls due to high
// memtable counts, which we want to avoid as it can cause latency hiccups of
// 100+ms for all write traffic.
func (io *ioLoadListener) adjustTokens(ctx context.Context, metrics StoreMetrics) {
	sas := io.kvRequester.getStoreAdmissionStats()
	// Copy the cumulative disk bandwidth values for later use.
	cumDiskBW := io.diskBW
	wt := metrics.Flush.WriteThroughput
	wt.Subtract(io.cumFlushWriteThroughput)
	if wt.Bytes < 0 {
		// Ignore wrong stats. Can happen in tests.
		wt.Bytes = 0
	}
	cumCompactionStats := computeCumStoreCompactionStats(metrics.Metrics)

	res := io.adjustTokensInner(ctx, io.ioLoadListenerState,
		metrics.Levels[0], metrics.WriteStallCount, cumCompactionStats,
		metrics.WAL.Failover.SecondaryWriteDuration, wt,
		L0FileCountOverloadThreshold.Get(&io.settings.SV),
		L0SubLevelCountOverloadThreshold.Get(&io.settings.SV),
		L0MinimumSizePerSubLevel.Get(&io.settings.SV),
		MinFlushUtilizationFraction.Get(&io.settings.SV),
		metrics.MemTable.Size,
		metrics.MemTableSizeForStopWrites,
	)
	io.adjustTokensResult = res
	cumIngestedBytes := cumLSMIngestedBytes(metrics.Metrics)

	// Disk Bandwidth tokens.
	elasticBWMaxUtil := ElasticBandwidthMaxUtil.Get(&io.settings.SV)
	intDiskLoadInfo := computeIntervalDiskLoadInfo(
		cumDiskBW.bytesRead, cumDiskBW.bytesWritten, metrics.DiskStats, elasticBWMaxUtil)
	diskTokensUsed := io.kvGranter.getDiskTokensUsedAndReset()
	if metrics.DiskStats.ProvisionedBandwidth > 0 {
		tokens := io.diskBandwidthLimiter.computeElasticTokens(
			intDiskLoadInfo, diskTokensUsed)
		io.diskWriteTokens = tokens.writeByteTokens
		io.diskWriteTokensAllocated = 0
		io.diskReadTokens = tokens.readByteTokens
		io.diskWriteTokensAllocated = 0
	}
	if metrics.DiskStats.ProvisionedBandwidth == 0 ||
		!DiskBandwidthTokensForElasticEnabled.Get(&io.settings.SV) {
		io.diskWriteTokens = unlimitedTokens
		// Currently, disk read tokens are only used to assess how many tokens were
		// deducted from the writes bucket to account for future reads. A 0 value
		// here represents that.
		io.diskReadTokens = 0
	}
	io.diskBW.bytesRead = metrics.DiskStats.BytesRead
	io.diskBW.bytesWritten = metrics.DiskStats.BytesWritten

	io.perWorkTokenEstimator.updateEstimates(
		metrics.Levels[0], cumIngestedBytes, metrics.DiskStats.BytesWritten, sas,
		io.aux.recentUnflushedMemTableTooLarge)
	io.copyAuxEtcFromPerWorkEstimator()
	requestEstimates := io.perWorkTokenEstimator.getStoreRequestEstimatesAtAdmission()
	io.kvRequester.setStoreRequestEstimates(requestEstimates)
	l0WriteLM, l0IngestLM, ingestLM, writeAmpLM := io.perWorkTokenEstimator.getModelsAtDone()
	io.kvGranter.setLinearModels(l0WriteLM, l0IngestLM, ingestLM, writeAmpLM)
	if io.aux.doLogFlush || io.diskBandwidthLimiter.state.diskBWUtil > 0.8 || log.V(1) {
		log.Infof(ctx, "IO overload: %s; %s", io.adjustTokensResult, io.diskBandwidthLimiter)
	}
}

// copyAuxEtcFromPerWorkEstimator copies the auxiliary and other numerical
// state from io.perWorkTokenEstimator. This is helpful in keeping all the
// numerical state for understanding the behavior of ioLoadListener and its
// helpers in one place for simplicity of logging.
func (io *ioLoadListener) copyAuxEtcFromPerWorkEstimator() {
	// Copy the aux so that the printing story is simplified.
	io.adjustTokensResult.aux.perWorkTokensAux = io.perWorkTokenEstimator.aux
	requestEstimates := io.perWorkTokenEstimator.getStoreRequestEstimatesAtAdmission()
	io.adjustTokensResult.requestEstimates = requestEstimates
	l0WriteLM, l0IngestLM, ingestLM, writeAmpLM := io.perWorkTokenEstimator.getModelsAtDone()
	io.adjustTokensResult.l0WriteLM = l0WriteLM
	io.adjustTokensResult.l0IngestLM = l0IngestLM
	io.adjustTokensResult.ingestLM = ingestLM
	io.adjustTokensResult.writeAmpLM = writeAmpLM
}

type tokenKind int8

const (
	compactionTokenKind tokenKind = iota
	flushTokenKind
)

// adjustTokensAuxComputations encapsulates auxiliary numerical state for
// ioLoadListener that is helpful for understanding its behavior.
type adjustTokensAuxComputations struct {
	intL0AddedBytes     int64
	intL0CompactedBytes int64

	intFlushTokens      float64
	intFlushUtilization float64
	intWriteStalls      int64

	intWALFailover bool

	prevTokensUsed                  int64
	prevTokensUsedByElasticWork     int64
	tokenKind                       tokenKind
	usedCompactionTokensLowerBound  bool
	recentUnflushedMemTableTooLarge bool

	perWorkTokensAux perWorkTokensAux
	doLogFlush       bool
}

// adjustTokensInner is used for computing tokens based on compaction and
// flush bottlenecks.
func (io *ioLoadListener) adjustTokensInner(
	ctx context.Context,
	prev ioLoadListenerState,
	l0Metrics pebble.LevelMetrics,
	cumWriteStallCount int64,
	cumCompactionStats cumStoreCompactionStats,
	cumWALSecondaryWriteDuration time.Duration,
	flushWriteThroughput pebble.ThroughputMetric,
	threshNumFiles, threshNumSublevels int64,
	l0MinSizePerSubLevel int64,
	minFlushUtilTargetFraction float64,
	memTableSize uint64,
	memTableSizeForStopWrites uint64,
) adjustTokensResult {
	ioThreshold := &admissionpb.IOThreshold{
		L0NumFiles:               l0Metrics.NumFiles,
		L0NumFilesThreshold:      threshNumFiles,
		L0NumSubLevels:           int64(l0Metrics.Sublevels),
		L0NumSubLevelsThreshold:  threshNumSublevels,
		L0Size:                   l0Metrics.Size,
		L0MinimumSizePerSubLevel: l0MinSizePerSubLevel,
	}
	unflushedMemTableTooLarge := memTableSize > memTableSizeForStopWrites
	// If it was too large in the last sample 15s ago, and is not large now, the
	// stats will still be skewed towards showing disproportionate L0 bytes
	// added compared to incoming writes to Pebble. So we include this limited
	// history.
	recentUnflushedMemTableTooLarge := unflushedMemTableTooLarge || io.unflushedMemTableTooLarge

	curL0Bytes := l0Metrics.Size
	cumL0AddedBytes := l0Metrics.BytesFlushed + l0Metrics.BytesIngested
	// L0 growth over the last interval.
	intL0AddedBytes := int64(cumL0AddedBytes) - int64(prev.cumL0AddedBytes)
	if intL0AddedBytes < 0 {
		// intL0AddedBytes is a simple delta computation over individually cumulative
		// stats, so should not be negative.
		log.Warningf(ctx, "intL0AddedBytes %d is negative", intL0AddedBytes)
		intL0AddedBytes = 0
	}
	// intL0CompactedBytes are due to finished compactions.
	intL0CompactedBytes := prev.curL0Bytes + intL0AddedBytes - curL0Bytes
	if intL0CompactedBytes < 0 {
		// Ignore potential inconsistencies across cumulative stats and current L0
		// bytes (gauge).
		intL0CompactedBytes = 0
	}
	io.l0CompactedBytes.Inc(intL0CompactedBytes)
	l0CompactionTokensLowerBound := computeL0CompactionTokensLowerBound(
		io.cumCompactionStats, cumCompactionStats)
	usedCompactionTokensLowerBound := false

	const alpha = 0.5

	// intWALFailover captures whether there were any writes to the secondary
	// WAL location in the last interval. WAL failover indicates that the
	// primary WAL location, which is also the location to which the store
	// flushes and compacts, may be unhealthy. If it is unhealthy, flushes and
	// compactions can stall, which can result in artificially low token counts
	// for flushes and compactions, which can unnecessarily throttle work. It is
	// also possible that the primary WAL location was transiently observed to
	// be slow, and flushes and compactions are mostly unaffected, and may even
	// be increasing in their rate, during WAL failover, if the workload is
	// increasing its write rate.
	//
	// We make the assumption that failover will be very aggressive compared to
	// the interval at which this token computation is happening (15s). An
	// UnhealthyOperationLatencyThreshold of 100ms (the default) means that an
	// interval in which intWALFailover was false could at worst have had its
	// last 100ms have stalled flushes/compactions. So the throughput observed
	// here will be 99.3% of what would have been possible with a healthy
	// primary, which is considered acceptable.
	//
	// We also make the assumption that failback will be reasonably aggressive
	// once the primary is considered healthy (HealthyInterval uses the default
	// of 15s). So a disk stall in the primary that lasts 30s, will cause WAL
	// failover for ~45s, and a disk stall for 1s will cause failover for ~16s.
	// The latter (16s) is short enough that we could potentially allow
	// unlimited tokens during failover. The concern is the former case, where
	// unlimited tokens could result in excessive admission into L0. So the
	// default behavior when intWALFailover is true is to (a) continue using the
	// compaction tokens from before the failover, unless the compaction rate is
	// increasing (b) not constrain flush tokens, (c) constrain elastic traffic
	// to effectively 0 tokens. We allow this behavior to be overridden to have
	// unlimited tokens.
	intWALFailover := cumWALSecondaryWriteDuration-io.cumWALSecondaryWriteDuration > 0
	var smoothedIntL0CompactedBytes int64
	var updatedSmoothedIntL0CompactedBytes bool
	if intWALFailover && intL0CompactedBytes < prev.smoothedIntL0CompactedBytes {
		// Reuse previous smoothed value since the decrease in compaction bytes
		// could be due to an unhealthy primary WAL location.
		smoothedIntL0CompactedBytes = prev.smoothedIntL0CompactedBytes
	} else {
		// Compaction scheduling can be uneven in prioritizing L0 for compactions,
		// so smooth out what is being removed by compactions.
		smoothedIntL0CompactedBytes = int64(alpha*float64(intL0CompactedBytes) +
			(1-alpha)*float64(prev.smoothedIntL0CompactedBytes))
		updatedSmoothedIntL0CompactedBytes = true
	}

	// Flush tokens:
	//
	// Write stalls happen when flushing of memtables is a bottleneck.
	//
	// Computing Flush Tokens:
	// Flush can go from not being the bottleneck in one 15s interval
	// (adjustmentInterval) to being the bottleneck in the next 15s interval
	// (e.g. when L0 falls below the unhealthy threshold and compaction tokens
	// become unlimited). So the flush token limit has to react quickly (cannot
	// afford to wait for multiple 15s intervals). We've observed that if we
	// normalize the flush rate based on flush loop utilization (the PeakRate
	// computation below), and use that to compute flush tokens, the token
	// counts are quite stable. Here are two examples, showing this steady token
	// count computed using PeakRate of the flush ThroughputMetric, despite
	// changes in flush loop utilization (the util number below).
	//
	// Example 1: Case where IO bandwidth was not a bottleneck
	// flush: tokens: 2312382401, util: 0.90
	// flush: tokens: 2345477107, util: 0.31
	// flush: tokens: 2317829891, util: 0.29
	// flush: tokens: 2428387843, util: 0.17
	//
	// Example 2: Case where IO bandwidth became a bottleneck (and mean fsync
	// latency was fluctuating between 1ms and 4ms in the low util to high util
	// cases).
	//
	// flush: tokens: 1406132615, util: 1.00
	// flush: tokens: 1356476227, util: 0.64
	// flush: tokens: 1374880806, util: 0.24
	// flush: tokens: 1328578534, util: 0.96
	//
	// Hence, using PeakRate as a basis for computing flush tokens seems sound.
	// The other important question is what fraction of PeakRate avoids write
	// stalls. It is likely less than 100% since while a flush is ongoing,
	// memtables can accumulate and cause a stall. For example, we have observed
	// write stalls at 80% of PeakRate. The fraction depends on configuration
	// parameters like MemTableStopWritesThreshold (defaults to 4 in
	// CockroachDB), and environmental and workload factors like how long a
	// flush takes to flush a single 64MB memtable. Instead of trying to measure
	// and adjust for these, we use a simple multiplier,
	// flushUtilTargetFraction. By default, flushUtilTargetFraction ranges
	// between 0.5 and 1.5. The lower bound is configurable via
	// admission.min_flush_util_percent and if configured above the upper bound,
	// the upper bound will be ignored and the target fraction will not be
	// dynamically adjusted. The dynamic adjustment logic uses an additive step
	// size of flushUtilTargetFractionIncrement (0.025), with the following
	// logic:
	// - Reduce the fraction if there is a write-stall. The reduction may use a
	//   small multiple of flushUtilTargetFractionIncrement. This is so that
	//   this probing spends more time below the threshold where write stalls
	//   occur.
	// - Increase fraction if no write-stall and flush tokens were almost all
	//   used.
	//
	// This probing unfortunately cannot eliminate write stalls altogether.
	// Future improvements could use more history to settle on a good
	// flushUtilTargetFraction for longer, or use some measure of how close we
	// are to a write-stall to stop the increase.
	//
	// Ingestion and flush tokens:
	//
	// Ingested sstables do not utilize any flush capacity. Consider 2 cases:
	// - sstable ingested into L0: there was either data overlap with L0, or
	//   file boundary overlap with L0-L6. To be conservative, lets assume there
	//   was data overlap, and that this data overlap extended into the memtable
	//   at the time of ingestion. Memtable(s) would have been force flushed to
	//   handle such overlap. The cost of flushing a memtable is based on how
	//   much of the allocated memtable capacity is used, so an early flush
	//   seems harmless. However, write stalls are based on allocated memtable
	//   capacity, so there is a potential negative interaction of these forced
	//   flushes since they cause additional memtable capacity allocation.
	// - sstable ingested into L1-L6: there was no data overlap with L0, which
	//   implies that there was no reason to flush memtables.
	//
	// Since there is some interaction noted in bullet 1, and because it
	// simplifies the admission control token behavior, we use flush tokens in
	// an identical manner as compaction tokens -- to be consumed by all data
	// flowing into L0. Some of this conservative choice will be compensated for
	// by flushUtilTargetFraction (when the mix of ingestion and actual flushes
	// are stable). Another thing to note is that compactions out of L0 are
	// typically the more persistent bottleneck than flushes for the following
	// reason:
	// There is a dedicated flush thread. With a maximum compaction concurrency
	// of C, we have up to C threads dedicated to handling the write-amp of W
	// (caused by rewriting the same data). So C/(W-1) threads on average are
	// reading the original data (that will be rewritten W-1 times). Since L0
	// can have multiple overlapping files, and intra-L0 compactions are usually
	// avoided, we can assume (at best) that the original data (in L0) is being
	// read only when compacting to levels lower than L0. That is, C/(W-1)
	// threads are reading from L0 to compact to levels lower than L0. Since W
	// can be 20+ and C defaults to 3 (we plan to dynamically adjust C but one
	// can expect C to be <= 10), C/(W-1) < 1. So the main reason we are
	// considering flush tokens is transient flush bottlenecks, and workloads
	// where W is small.

	// Compute flush utilization for this interval. A very low flush utilization
	// will cause flush tokens to be unlimited.
	intFlushUtilization := float64(0)
	if flushWriteThroughput.WorkDuration > 0 {
		intFlushUtilization = float64(flushWriteThroughput.WorkDuration) /
			float64(flushWriteThroughput.WorkDuration+flushWriteThroughput.IdleDuration)
	}
	// Compute flush tokens for this interval that would cause 100% utilization.
	intFlushTokens := float64(flushWriteThroughput.PeakRate()) * adjustmentInterval
	intWriteStalls := cumWriteStallCount - prev.cumWriteStallCount

	// Ensure flushUtilTargetFraction is in the configured bounds. This also
	// does lazy initialization.
	const maxFlushUtilTargetFraction = 1.5
	flushUtilTargetFraction := prev.flushUtilTargetFraction
	if flushUtilTargetFraction == 0 {
		// Initialization: use the init configured fraction.
		flushUtilTargetFraction = minFlushUtilTargetFraction
		// 1.0 is a high enough value -- we've observed write stalls at ~0.65 when
		// running kv0. Which is why we don't use maxFlushUtilTargetFraction as
		// the initial value.
		const initFlushUtilTargetFraction = 1.0
		if flushUtilTargetFraction < initFlushUtilTargetFraction {
			flushUtilTargetFraction = initFlushUtilTargetFraction
		}
	} else if flushUtilTargetFraction < minFlushUtilTargetFraction {
		// The min can be changed in a running system, so we bump up to conform to
		// the min.
		flushUtilTargetFraction = minFlushUtilTargetFraction
	}
	numFlushTokens := int64(unlimitedTokens)
	// doLogFlush becomes true if something interesting is done here.
	doLogFlush := false
	smoothedNumFlushTokens := prev.smoothedNumFlushTokens
	const flushUtilIgnoreThreshold = 0.1
	if intFlushUtilization > flushUtilIgnoreThreshold && !intWALFailover {
		if smoothedNumFlushTokens == 0 {
			// Initialization.
			smoothedNumFlushTokens = intFlushTokens
		} else {
			smoothedNumFlushTokens = alpha*intFlushTokens + (1-alpha)*prev.smoothedNumFlushTokens
		}
		const flushUtilTargetFractionIncrement = 0.025
		// Have we used, over the last (15s) cycle, more than 90% of the tokens we
		// would give out for the next cycle? If yes, highTokenUsage is true.
		highTokenUsage :=
			float64(prev.byteTokensUsed) >= 0.9*smoothedNumFlushTokens*flushUtilTargetFraction
		if intWriteStalls > 0 {
			// Try decrease since there were write-stalls.
			numDecreaseSteps := 1
			// These constants of 5, 3, 2, 2 were found to work reasonably well,
			// without causing large decreases. We need better benchmarking to tune
			// such constants.
			if intWriteStalls >= 5 {
				numDecreaseSteps = 3
			} else if intWriteStalls >= 2 {
				numDecreaseSteps = 2
			}
			for i := 0; i < numDecreaseSteps; i++ {
				if flushUtilTargetFraction >= minFlushUtilTargetFraction+flushUtilTargetFractionIncrement {
					flushUtilTargetFraction -= flushUtilTargetFractionIncrement
					doLogFlush = true
				} else {
					break
				}
			}
		} else if flushUtilTargetFraction < maxFlushUtilTargetFraction-flushUtilTargetFractionIncrement &&
			intWriteStalls == 0 && highTokenUsage {
			// No write-stalls, and token usage was high, so give out more tokens.
			flushUtilTargetFraction += flushUtilTargetFractionIncrement
			doLogFlush = true
		}
		if highTokenUsage {
			doLogFlush = true
		}
		flushTokensFloat := flushUtilTargetFraction * smoothedNumFlushTokens
		if flushTokensFloat < float64(unlimitedTokens) {
			numFlushTokens = int64(flushTokensFloat)
		}
		// Else avoid overflow by using the previously set unlimitedTokens. This
		// should not really happen.
	}
	// Else intFlushUtilization is too low or WAL failover is active. We
	// don't want to make token determination based on a very low utilization,
	// or when flushes are stalled, so we hand out unlimited
	// tokens. Note that flush utilization has been observed to fluctuate from
	// 0.16 to 0.9 in a single interval, when compaction tokens are not limited,
	// hence we have set flushUtilIgnoreThreshold to a very low value. If we've
	// erred towards it being too low, we run the risk of computing incorrect
	// tokens. If we've erred towards being too high, we run the risk of giving
	// out unlimitedTokens and causing write stalls.

	// We constrain admission based on compactions, if the store is over the L0
	// threshold.
	var totalNumByteTokens int64
	var smoothedCompactionByteTokens float64

	score, _ := ioThreshold.Score()
	// Multiplying score by 2 for ease of calculation.
	score *= 2
	// We define four levels of load:
	// Let C be smoothedIntL0CompactedBytes.
	//
	// Underload: Score is less than 0.5, which means sublevels is less than 5.
	// In this case, we don't limit compaction tokens. Flush tokens will likely
	// become the limit.
	//
	// Low load: Score is >= 0.5 and score is less than 1. In this case, we limit
	// compaction tokens, and interpolate between C tokens when score is 1, and
	// 2C tokens when score is 0.5.
	//
	// Medium load: Score is >= 1 and < 2. We limit compaction tokens, and limit
	// them between C and C/2 tokens when score is 1 and 2 respectively.
	//
	// Overload: Score is >= 2. We limit compaction tokens, and limit tokens to
	// at most C/2 tokens.
	if score < 0.5 {
		if !updatedSmoothedIntL0CompactedBytes {
			smoothedCompactionByteTokens = prev.smoothedCompactionByteTokens
		} else {
			// Underload. Maintain a smoothedCompactionByteTokens based on what was
			// removed, so that when we go over the threshold we have some history.
			// This is also useful when we temporarily dip below the threshold --
			// we've seen extreme situations with alternating 15s intervals of above
			// and below the threshold.
			numTokens := intL0CompactedBytes
			// Smooth it. Unlike the else block below, we smooth using
			// intL0CompactedBytes, to be more responsive.
			smoothedCompactionByteTokens = alpha*float64(numTokens) + (1-alpha)*prev.smoothedCompactionByteTokens
		}
		totalNumByteTokens = unlimitedTokens
	} else {
		doLogFlush = true
		if !updatedSmoothedIntL0CompactedBytes {
			smoothedCompactionByteTokens = prev.smoothedCompactionByteTokens
		} else {
			var fTotalNumByteTokens float64
			if score >= 2 {
				// Overload.
				//
				// Don't admit more byte work than we can remove via compactions.
				// totalNumByteTokens tracks our goal for admission. Scale down
				// since we want to get under the thresholds over time.
				fTotalNumByteTokens = float64(smoothedIntL0CompactedBytes / 2.0)
			} else if score >= 0.5 && score < 1 {
				// Low load. Score in [0.5, 1). Tokens should be
				// smoothedIntL0CompactedBytes at 1, and 2 * smoothedIntL0CompactedBytes
				// at 0.5.
				fTotalNumByteTokens = -score*(2*float64(smoothedIntL0CompactedBytes)) + 3*float64(smoothedIntL0CompactedBytes)
				if fTotalNumByteTokens < float64(l0CompactionTokensLowerBound) {
					fTotalNumByteTokens = float64(l0CompactionTokensLowerBound)
					usedCompactionTokensLowerBound = true
				}
			} else {
				// Medium load. Score in [1, 2). We use linear interpolation from
				// medium load to overload, to slowly give out fewer tokens as we
				// move towards overload.
				halfSmoothedBytes := float64(smoothedIntL0CompactedBytes / 2.0)
				fTotalNumByteTokens = -score*halfSmoothedBytes + 3*halfSmoothedBytes
			}
			smoothedCompactionByteTokens = alpha*fTotalNumByteTokens + (1-alpha)*prev.smoothedCompactionByteTokens
		}
		if float64(unlimitedTokens) < smoothedCompactionByteTokens {
			// Avoid overflow. This should not really happen.
			totalNumByteTokens = unlimitedTokens
		} else {
			totalNumByteTokens = int64(smoothedCompactionByteTokens)
		}
	}

	totalNumElasticByteTokens := int64(unlimitedTokens)
	// NB: score == (num-sublevels / 20) * 2 = num-sublevels/10 (we are ignoring
	// the rare case where score is determined by file count). So score >= 0.2
	// means that we start shaping when there are 2 sublevels.
	if score >= 0.2 {
		doLogFlush = true
		if intWALFailover {
			totalNumElasticByteTokens = 1
		} else {
			// Use a linear function with slope of -1.25 and compaction tokens of
			// 1.25*compaction-bandwidth at score of 0.2. At a score of 0.6 (6
			// sublevels) the tokens will be 0.75*compaction-bandwidth. Experimental
			// results show the sublevels hovering around 4, as expected.
			//
			// NB: at score >= 1.2 (12 sublevels), there are 0 elastic tokens.
			//
			// NB: we are not using l0CompactionTokensLowerBound at all for elastic
			// tokens. This is because that lower bound is useful primarily when L0 is
			// not seeing compactions because a compaction backlog has accumulated in
			// other levels. For elastic work, we would rather clear that compaction
			// backlog than admit some elastic work.
			totalNumElasticByteTokens = int64(float64(smoothedIntL0CompactedBytes) *
				(1.25 - 1.25*(score-0.2)))
		}
		totalNumElasticByteTokens = max(totalNumElasticByteTokens, 1)
	}
	if recentUnflushedMemTableTooLarge {
		// There is a large flush backlog -- this will typically happen during or
		// immediately after WAL failover. Don't allow elastic work to get more
		// than the bytes we compacted out of L0. There are other choices we could
		// have made here (a) give 1 token to elastic work, effectively throttling
		// it down to zero, (b) chosen some other fraction of intL0CompactedBytes.
		// With (a), we run the risk of rapidly switching back and forth between
		// giving elastic work 1 token and unlimitedTokens, which could be worse
		// than staying in this mode of a flush backlog. With (b), we don't have a
		// good idea of what fraction would be appropriate. Note that this is
		// mainly a way to avoid overloading the disk. If disk bandwidth based
		// tokens are enabled, those should be observing the cost of large ongoing
		// flushes and throttling elastic work, and there should be less need of
		// this mechanism.
		if totalNumElasticByteTokens > intL0CompactedBytes {
			doLogFlush = true
			totalNumElasticByteTokens = intL0CompactedBytes
		}
	}
	// Use the minimum of the token count calculated using compactions and
	// flushes.
	tokenKind := compactionTokenKind
	if totalNumByteTokens > numFlushTokens {
		totalNumByteTokens = numFlushTokens
		// Reduce the flush tokens for elastic traffic, since write stalls can be
		// dangerous. 0.8 was chosen somewhat arbitrarily.
		numElasticFlushTokens := int64(0.8 * float64(numFlushTokens))
		if numElasticFlushTokens < totalNumElasticByteTokens {
			totalNumElasticByteTokens = numElasticFlushTokens
		}
		tokenKind = flushTokenKind
	}
	if totalNumElasticByteTokens > totalNumByteTokens {
		totalNumElasticByteTokens = totalNumByteTokens
	}
	if intWALFailover && walFailoverUnlimitedTokens.Get(&io.settings.SV) {
		totalNumByteTokens = unlimitedTokens
		totalNumElasticByteTokens = unlimitedTokens
	}

	io.l0TokensProduced.Inc(totalNumByteTokens)

	// Install the latest cumulative stats.
	return adjustTokensResult{
		ioLoadListenerState: ioLoadListenerState{
			cumL0AddedBytes:              cumL0AddedBytes,
			curL0Bytes:                   curL0Bytes,
			cumWriteStallCount:           cumWriteStallCount,
			cumCompactionStats:           cumCompactionStats,
			cumWALSecondaryWriteDuration: cumWALSecondaryWriteDuration,
			unflushedMemTableTooLarge:    unflushedMemTableTooLarge,
			smoothedIntL0CompactedBytes:  smoothedIntL0CompactedBytes,
			smoothedCompactionByteTokens: smoothedCompactionByteTokens,
			smoothedNumFlushTokens:       smoothedNumFlushTokens,
			flushUtilTargetFraction:      flushUtilTargetFraction,
			totalNumByteTokens:           totalNumByteTokens,
			byteTokensAllocated:          0,
			byteTokensUsed:               0,
			byteTokensUsedByElasticWork:  0,
			totalNumElasticByteTokens:    totalNumElasticByteTokens,
			elasticByteTokensAllocated:   0,
		},
		aux: adjustTokensAuxComputations{
			intL0AddedBytes:                 intL0AddedBytes,
			intL0CompactedBytes:             intL0CompactedBytes,
			intFlushTokens:                  intFlushTokens,
			intFlushUtilization:             intFlushUtilization,
			intWriteStalls:                  intWriteStalls,
			intWALFailover:                  intWALFailover,
			prevTokensUsed:                  prev.byteTokensUsed,
			prevTokensUsedByElasticWork:     prev.byteTokensUsedByElasticWork,
			tokenKind:                       tokenKind,
			usedCompactionTokensLowerBound:  usedCompactionTokensLowerBound,
			recentUnflushedMemTableTooLarge: recentUnflushedMemTableTooLarge,
			doLogFlush:                      doLogFlush,
		},
		ioThreshold: ioThreshold,
	}
}

// adjustTokensResult encapsulates all the numerical state of ioLoadListener.
type adjustTokensResult struct {
	ioLoadListenerState
	requestEstimates storeRequestEstimates
	l0WriteLM        tokensLinearModel
	l0IngestLM       tokensLinearModel
	ingestLM         tokensLinearModel
	writeAmpLM       tokensLinearModel
	aux              adjustTokensAuxComputations
	ioThreshold      *admissionpb.IOThreshold // never nil
}

func (res adjustTokensResult) SafeFormat(p redact.SafePrinter, _ rune) {
	ib := humanizeutil.IBytes
	// NB: "≈" indicates smoothed quantities.
	p.Printf("compaction score %v (%d ssts, %d sub-levels), ", res.ioThreshold, res.ioThreshold.L0NumFiles, res.ioThreshold.L0NumSubLevels)
	var recentFlushBackogStr string
	if res.aux.recentUnflushedMemTableTooLarge {
		recentFlushBackogStr = " (flush-backlog) "
	}
	p.Printf("L0 growth %s%s (write %s (ignored %s) ingest %s (ignored %s)): ",
		ib(res.aux.intL0AddedBytes),
		redact.SafeString(recentFlushBackogStr),
		ib(res.aux.perWorkTokensAux.intL0WriteBytes),
		ib(res.aux.perWorkTokensAux.intL0IgnoredWriteBytes),
		ib(res.aux.perWorkTokensAux.intL0IngestedBytes),
		ib(res.aux.perWorkTokensAux.intL0IgnoredIngestedBytes))
	// Writes to L0 that we expected because requests told admission control.
	// This is the "easy path", from an estimation perspective, if all regular
	// writes accurately tell us what they write, and all ingests tell us what
	// they ingest and all of ingests into L0.
	p.Printf("requests %d (%d bypassed) with ", res.aux.perWorkTokensAux.intWorkCount,
		res.aux.perWorkTokensAux.intBypassedWorkCount)
	p.Printf("%s acc-write (%s bypassed) + ",
		ib(res.aux.perWorkTokensAux.intL0WriteAccountedBytes),
		ib(res.aux.perWorkTokensAux.intL0WriteBypassedAccountedBytes))
	// Ingestion bytes that we expected because requests told admission control.
	p.Printf("%s acc-ingest (%s bypassed) + ",
		ib(res.aux.perWorkTokensAux.intIngestedAccountedBytes),
		ib(res.aux.perWorkTokensAux.intIngestedBypassedAccountedBytes))
	// Adjusted LSM writes and disk writes that were used for w-amp estimation.
	p.Printf("%s adjusted-LSM-writes + %s adjusted-disk-writes + ",
		ib(res.aux.perWorkTokensAux.intAdjustedLSMWrites),
		ib(res.aux.perWorkTokensAux.intAdjustedDiskWriteBytes))
	// The models we are fitting to compute tokens based on the reported size of
	// the write and ingest.
	p.Printf("write-model %.2fx+%s (smoothed %.2fx+%s) + ",
		res.aux.perWorkTokensAux.intL0WriteLinearModel.multiplier,
		ib(res.aux.perWorkTokensAux.intL0WriteLinearModel.constant),
		res.l0WriteLM.multiplier, ib(res.l0WriteLM.constant))
	p.Printf("ingested-model %.2fx+%s (smoothed %.2fx+%s) + ",
		res.aux.perWorkTokensAux.intL0IngestedLinearModel.multiplier,
		ib(res.aux.perWorkTokensAux.intL0IngestedLinearModel.constant),
		res.l0IngestLM.multiplier, ib(res.l0IngestLM.constant))
	p.Printf("write-amp-model %.2fx+%s (smoothed %.2fx+%s) + ",
		res.aux.perWorkTokensAux.intWriteAmpLinearModel.multiplier,
		ib(res.aux.perWorkTokensAux.intWriteAmpLinearModel.constant),
		res.writeAmpLM.multiplier, ib(res.writeAmpLM.constant))
	// The tokens used per request at admission time, when no size information
	// is known.
	p.Printf("at-admission-tokens %s, ", ib(res.requestEstimates.writeTokens))
	// How much got compacted out of L0 recently.
	p.Printf("compacted %s [≈%s], ", ib(res.aux.intL0CompactedBytes), ib(res.smoothedIntL0CompactedBytes))
	// The tokens computed for flush, based on observed flush throughput and
	// utilization.
	p.Printf("flushed %s [≈%s] (mult %.2f); ", ib(int64(res.aux.intFlushTokens)),
		ib(int64(res.smoothedNumFlushTokens)), res.flushUtilTargetFraction)
	p.Printf("admitting ")
	if res.aux.intWALFailover {
		p.Printf("(WAL failover) ")
	}
	if n, m := res.ioLoadListenerState.totalNumByteTokens,
		res.ioLoadListenerState.totalNumElasticByteTokens; n < unlimitedTokens {
		p.Printf("%s (rate %s/s) (elastic %s rate %s/s)", ib(n), ib(n/adjustmentInterval), ib(m),
			ib(m/adjustmentInterval))
		switch res.aux.tokenKind {
		case compactionTokenKind:
			// NB: res.smoothedCompactionByteTokens  is the same as
			// res.ioLoadListenerState.totalNumByteTokens (printed above) when
			// res.aux.tokenKind == compactionTokenKind.
			lowerBoundBoolStr := ""
			if res.aux.usedCompactionTokensLowerBound {
				lowerBoundBoolStr = "(used token lower bound)"
			}
			p.Printf(" due to L0 growth%s", lowerBoundBoolStr)
		case flushTokenKind:
			p.Printf(" due to memtable flush (multiplier %.3f)", res.flushUtilTargetFraction)
		}
		p.Printf(" (used total: %s elastic %s)", ib(res.aux.prevTokensUsed),
			ib(res.aux.prevTokensUsedByElasticWork))
	} else if m < unlimitedTokens {
		p.Printf("elastic %s (rate %s/s) due to L0 growth", ib(m), ib(m/adjustmentInterval))
	} else {
		p.SafeString("all")
	}
	p.Printf("; write stalls %d", res.aux.intWriteStalls)
}

func (res adjustTokensResult) String() string {
	return redact.StringWithoutMarkers(res)
}
