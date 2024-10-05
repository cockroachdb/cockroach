// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// The functionality in this file is geared towards preventing chronic overload
// of disk bandwidth which typically results in severely high latency for all work.
//
// For now, we assume that:
// - There is a provisioned limit on the sum of read and write bandwidth. This
//   limit is allowed to change. This is true for block devices of major cloud
//   providers.
// - Admission control can only shape the rate of admission of writes. Writes
//   also cause reads, since compactions do reads and writes.
//
// There are multiple challenges:
// - We are unable to precisely track the causes of disk read bandwidth, since
//   we do not have observability into what reads missed the OS page cache.
//   That is we don't know how much of the reads were due to incoming reads
//   (that we don't shape) and how much due to compaction read bandwidth.
//
// - We don't shape incoming reads.
//
// - There can be a large lag (1+min) between the shaping of incoming writes,
//   and when it affects actual writes in the system, since compaction backlog
//   can build up in various levels of the LSM store.
//
// - Signals of overload are coarse, since we cannot view all the internal
//   queues that can build up due to resource overload. For instance,
//   different examples of bandwidth saturation exhibit very different
//   latency effects, presumably because the queue buildup is different. So it
//   is non-trivial to approach full utilization without risking high latency.
//
// Due to these challenges, we adopt a goal of simplicity of design, and
// strong abstraction boundaries.
//
// - The disk load is abstracted using an enum, diskLoadLevel. The
//   diskLoadWatcher, that maps load signals to this enum, can be evolved
//   independently.
//
// - The approach uses easy to understand small multiplicative increase and
//   large multiplicative decrease, (unlike what we do for flush and
//   compaction tokens, where we try to more precisely calculate the
//   sustainable rates).
//
// Since we are using a simple approach that is somewhat coarse in its behavior,
// we start by limiting its application to two kinds of writes (the second one
// is future work, and not yet implemented):
//
// - Incoming writes that are deemed "elastic": This can be done by
//   introducing a work-class (in addition to admissionpb.WorkPriority), or by
//   implying a work-class from the priority (e.g. priorities < NormalPri are
//   deemed elastic).
//
// - Optional compactions: We assume that the LSM store is configured with a
//   ceiling on number of regular concurrent compactions, and if it needs more
//   it can request resources for additional (optional) compactions. These
//   latter compactions can be limited by this approach. See
//   https://github.com/cockroachdb/pebble/issues/1329 for motivation.
//   TODO(sumeer): this compaction control is not yet done, though how to do
//   it is included in the prototype in
//   https://github.com/cockroachdb/cockroach/pull/82813
//
// Extending this to all incoming writes is future work.

// The load level of a disk.
type diskLoadLevel int8

const (
	// diskLoadLow implies no need to shape anything.
	diskLoadLow diskLoadLevel = iota
	// diskLoadModerate implies shaping and small multiplicative increase.
	diskLoadModerate
	// diskLoadHigh implies shaping and hold steady.
	diskLoadHigh
	// diskLoadOverload implies shaping and large multiplicative decrease.
	diskLoadOverload
)

func diskLoadLevelString(level diskLoadLevel) redact.SafeString {
	switch level {
	case diskLoadLow:
		return "low"
	case diskLoadModerate:
		return "moderate"
	case diskLoadHigh:
		return "high"
	case diskLoadOverload:
		return "overload"
	}
	return ""
}

// diskLoadWatcher computes the diskLoadLevel based on provided stats.
type diskLoadWatcher struct {
	lastInterval intervalDiskLoadInfo
	lastUtil     float64
	loadLevel    diskLoadLevel
}

// intervalDiskLoadInfo provides disk stats over an adjustmentInterval.
type intervalDiskLoadInfo struct {
	// readBandwidth is the measure disk read bandwidth in bytes/s.
	readBandwidth int64
	// writeBandwidth is the measured disk write bandwidth in bytes/s.
	writeBandwidth int64
	// provisionedBandwidth is the aggregate (read+write) provisioned bandwidth
	// in bytes/s.
	provisionedBandwidth int64
}

// setIntervalInfo is called at the same time as ioLoadListener.pebbleMetricsTick.
func (d *diskLoadWatcher) setIntervalInfo(load intervalDiskLoadInfo) {
	lastInterval := load
	util := float64(load.readBandwidth+load.writeBandwidth) / float64(load.provisionedBandwidth)
	// The constants and other heuristics in the following logic can seem
	// extremely arbitrary: they were subject to some tuning and evolution based
	// on the experiments in https://github.com/cockroachdb/cockroach/pull/82813
	// that used (a) an artificial provisioned bandwidth limit lower than the
	// actual, to see how well the system stayed within that limit, (b) an
	// actual provisioned bandwidth limit. The difficulty in general is that
	// small changes can have outsize influence if a higher number of
	// compactions start happening.
	var loadLevel diskLoadLevel
	const lowUtilThreshold = 0.3
	const moderateUtilThreshold = 0.7
	const highUtilThreshold = 0.95
	const highlyOverUtilizedThreshold = 2.0
	const smallDelta = 0.05
	if util < lowUtilThreshold {
		// Were at moderate or lower and have not increased significantly and the
		// lastUtil was also low, then we can afford to stop limiting tokens. We
		// are trying to carefully narrow this case since not limiting tokens can
		// blow up bandwidth.
		//
		// An alternative would be to never have unlimited tokens, since that
		// ensures there is always some reasonable bound in place. It may mean
		// that the initial tokens are insufficient and the tokens catch up to
		// what is needed with some lag, and during that time there is unnecessary
		// queueing. This downside could be avoided by ramping up faster. This
		// alternative is worth investigating.

		if d.loadLevel <= diskLoadModerate && util < d.lastUtil+smallDelta &&
			d.lastUtil < lowUtilThreshold {
			loadLevel = diskLoadLow
		} else {
			// util is increasing, or we just dropped from something higher than
			// moderate. Give it more time at moderate, where we will gradually
			// increase tokens.
			loadLevel = diskLoadModerate
		}
	} else if util < moderateUtilThreshold {
		// Wide band from [0.3,0.7) where we gradually increase tokens. Also, 0.7
		// is deliberately a lowish fraction since the effect on compactions can
		// lag and kick in later. We are ok with accepting a lower utilization for
		// elastic work to make progress.
		loadLevel = diskLoadModerate
	} else if util < highUtilThreshold ||
		(util < highlyOverUtilizedThreshold && util < d.lastUtil-smallDelta) {
		// Wide band from [0.7,0.95) where we will hold the number of tokens
		// steady. We don't want to overreact and decrease too early since
		// compaction bandwidth usage can be lumpy. For this same reason, if we
		// are trending downward, we want to hold. Note that util < 2 will always
		// be true in typical configurations where one cannot actually exceed
		// provisioned bandwidth -- but we also run experiments where we
		// artificially constrain the provisioned bandwidth, where this is useful.
		// And it is possible that some production settings may set a slightly
		// lower value of provisioned bandwidth, if they want to further reduce
		// the probability of hitting the real provisioned bandwidth due to
		// elastic work.
		loadLevel = diskLoadHigh
	} else {
		// Overloaded. We will reduce tokens.
		loadLevel = diskLoadOverload
	}
	*d = diskLoadWatcher{
		lastInterval: lastInterval,
		lastUtil:     util,
		loadLevel:    loadLevel,
	}
	// TODO(sumeer): Use the history of fsync latency and the value in the
	// current interval, and if high, increase the load level computed earlier.
	// We shouldn't rely fully on syncLatencyMicros since (a) sync latency could
	// arise due to an external unrelated outage, (b) some customers may set
	// fsync to be a noop. As an alternative to sync latency, we could also
	// consider looking at fluctuations of peak-rate that the WAL writer can
	// sustain.
}

func (d *diskLoadWatcher) getLoadLevel() diskLoadLevel {
	return d.loadLevel
}

func (d diskLoadWatcher) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Printf("disk bandwidth: read: %s/s, write: %s/s, provisioned: %s/s, util: %.2f",
		humanizeutil.IBytes(d.lastInterval.readBandwidth),
		humanizeutil.IBytes(d.lastInterval.writeBandwidth),
		humanizeutil.IBytes(d.lastInterval.provisionedBandwidth), d.lastUtil)
}

// intervalLSMInfo provides stats about the LSM over an adjustmentInterval.
type intervalLSMInfo struct {
	// Flushed bytes + Ingested bytes seen by the LSM. Ingested bytes incur the
	// cost of writing a sstable, even though that is done outside Pebble, so
	// ingestion is similar in cost to flushing. Ingested bytes don't cause WAL
	// writes, but we ignore that difference for simplicity, and just work with
	// the sum of flushed and ingested bytes.
	incomingBytes int64
	// regularTokensUsed and elasticTokensUsed are the byte tokens used for
	// regular and elastic work respectively. Each of these includes both
	// writes that will get flushed and ingested bytes. The
	// regularTokensUsed+elasticTokensUsed do not need to sum up to
	// incomingBytes, since these stats are produced by different sources.
	regularTokensUsed int64
	elasticTokensUsed int64
}

type diskBandwidthLimiterState struct {
	smoothedIncomingBytes   float64
	smoothedElasticFraction float64
	elasticTokens           int64

	prevElasticTokensUsed int64
}

// diskBandwidthLimiter produces tokens for elastic work.
type diskBandwidthLimiter struct {
	diskLoadWatcher diskLoadWatcher
	state           diskBandwidthLimiterState
}

func makeDiskBandwidthLimiter() diskBandwidthLimiter {
	return diskBandwidthLimiter{
		state: diskBandwidthLimiterState{
			elasticTokens: math.MaxInt64,
		},
	}
}

// computeElasticTokens is called every adjustmentInterval.
func (d *diskBandwidthLimiter) computeElasticTokens(
	ctx context.Context, id intervalDiskLoadInfo, il intervalLSMInfo,
) (elasticTokens int64) {
	d.diskLoadWatcher.setIntervalInfo(id)
	const alpha = 0.5
	prev := d.state
	smoothedIncomingBytes := alpha*float64(il.incomingBytes) + (1-alpha)*prev.smoothedIncomingBytes
	smoothedElasticFraction := prev.smoothedElasticFraction
	var intElasticFraction float64
	if il.regularTokensUsed+il.elasticTokensUsed > 0 {
		intElasticFraction =
			float64(il.elasticTokensUsed) / float64(il.regularTokensUsed+il.elasticTokensUsed)
		smoothedElasticFraction = alpha*intElasticFraction + (1-alpha)*prev.smoothedElasticFraction
	}
	intElasticBytes := int64(float64(il.incomingBytes) * intElasticFraction)
	ll := d.diskLoadWatcher.getLoadLevel()

	// The constants and other heuristics in the following logic can seem
	// arbitrary: they were subject to some tuning and evolution based on the
	// experiments in https://github.com/cockroachdb/cockroach/pull/82813 that
	// used (a) an artificial provisioned bandwidth limit lower than the actual,
	// to see how well the system stayed within that limit, (b) an actual
	// provisioned bandwidth limit. The difficulty in general is that small
	// changes can have outsize influence if a higher number of compactions
	// start happening, or the compaction backlog is cleared.
	//
	// TODO(sumeer): experiment with a more sophisticated controller for the
	// elastic token adjustment, e.g. a PID (Proportional-Integral-Derivative)
	// controller.
	doLog := true
	switch ll {
	case diskLoadLow:
		elasticTokens = math.MaxInt64
		if elasticTokens == prev.elasticTokens {
			doLog = false
		}
		// else we stay in the common case of low bandwidth usage.
	case diskLoadModerate:
		tokensFullyUtilized :=
			// elasticTokens == MaxInt64 is also considered fully utilized since we
			// can never fully utilize unlimited tokens.
			prev.elasticTokens == math.MaxInt64 ||
				(prev.elasticTokens > 0 && float64(il.elasticTokensUsed)/float64(prev.elasticTokens) >= 0.8)

		if tokensFullyUtilized {
			// Smoothed elastic bytes plus 10% of smoothedIncomingBytes is given to
			// elastic work. That is, we are increasing the total incoming bytes by
			// 10% (not just the elastic bytes by 10%). Note that each token
			// represents 1 incoming byte.
			elasticBytes := (smoothedElasticFraction + 0.1) * smoothedIncomingBytes
			// Sometimes we see the tokens not increasing even though we are staying
			// for multiple intervals at moderate. This is because the smoothed
			// fraction and incoming bytes can be decreasing. We do want to increase
			// tokens since we know there is spare capacity, so we try many ways
			// (that don't look at smoothed numbers only). Also, we sometimes come
			// here due to an overload=>moderate transition because compaction
			// bandwidth usage can be lumpy (high when there is a backlog and then
			// dropping severely) -- in that case we want to start increasing
			// immediately, since we have likely decreased too much.
			intBasedElasticTokens := (smoothedElasticFraction + 0.1) * float64(il.incomingBytes)
			elasticBytes = math.Max(elasticBytes, intBasedElasticTokens)
			elasticBytes = math.Max(elasticBytes, 1.1*float64(il.elasticTokensUsed))
			elasticTokens = int64(elasticBytes)
			if elasticTokens == 0 {
				// Don't get stuck in a situation where smoothedIncomingBytes are 0.
				elasticTokens = math.MaxInt64
			}
		} else {
			// No change.
			elasticTokens = prev.elasticTokens
		}
	case diskLoadHigh:
		// No change.
		elasticTokens = prev.elasticTokens
	case diskLoadOverload:
		// Sometimes we come here after a low => overload transition. The
		// intElasticBytes will be very high because tokens were unlimited. We
		// don't want to use that as the starting point of the decrease if the
		// smoothed value is lower. Hence, the min logic below, to try to dampen
		// the increase quickly.
		elasticTokens = int64(0.5 * math.Min(float64(intElasticBytes),
			smoothedElasticFraction*smoothedIncomingBytes))
	}
	// We can end up with 0 elastic tokens here -- e.g. if intElasticBytes was 0
	// but we were still overloaded because of compactions. The trouble with 0
	// elastic tokens is that if we don't admit anything, we cannot correct an
	// occasional poor estimate of the per-request bytes. So we decide to give
	// out at least 1 token. A single elastic request should not be too big for
	// this to matter.
	elasticTokens = max(1, elasticTokens)
	d.state = diskBandwidthLimiterState{
		smoothedIncomingBytes:   smoothedIncomingBytes,
		smoothedElasticFraction: smoothedElasticFraction,
		elasticTokens:           elasticTokens,
		prevElasticTokensUsed:   il.elasticTokensUsed,
	}
	if doLog {
		log.Infof(ctx, "%v", d)
	}
	return elasticTokens
}

func (d *diskBandwidthLimiter) SafeFormat(p redact.SafePrinter, _ rune) {
	ib := humanizeutil.IBytes
	level := d.diskLoadWatcher.getLoadLevel()
	p.Printf("diskBandwidthLimiter %s (%v): elastic-frac: %.2f, incoming: %s, "+
		"elastic-tokens (used %s): %s",
		diskLoadLevelString(level), d.diskLoadWatcher, d.state.smoothedElasticFraction,
		ib(int64(d.state.smoothedIncomingBytes)), ib(d.state.prevElasticTokensUsed),
		ib(d.state.elasticTokens))
}
