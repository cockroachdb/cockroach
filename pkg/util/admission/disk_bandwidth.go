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
	"math"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// The functionality in this file is geared towards preventing chronic overload
// of disk bandwidth which typically results in severely high latency for all work.

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
// - There can be a huge lag between the shaping of incoming writes, and when
//   it affects actual writes in the system, since compaction backlog can
//   build up in various levels of the LSM store.
//
// - Signals of overload are coarse, since we cannot view all the internal
//   queues that can build up due to resource overload. For instance,
//   different examples of bandwidth saturation exhibit wildly different
//   latency effects, presumably because the queue buildup is different. So it
//   is non-trivial to approach full utilization without risking high latency.
//
// Due to these challenges, and previous design attempts that were quite
// complicated (and incomplete), we adopt a goal of simplicity of design, and strong
// abstraction boundaries.
//
// - The disk load is abstracted using an enum. The diskLoadWatcher can be
//   evolved independently.
//
// - The approach uses easy to understand additive increase and multiplicative
//   decrease, (unlike what we do for flush and compaction tokens, where we
//   try to more precisely calculate the sustainable rates).
//
// Since we are using a simple approach that is somewhat coarse in its behavior,
// we start by limiting its application to two kinds of writes:
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
//
// Extending this to all incoming writes is future work.

type diskLoadLevel int8

const (
	// diskLoadLow implies no need to shape anything.
	diskLoadLow diskLoadLevel = iota
	// diskLoadModerate implies shaping and additive increase.
	diskLoadModerate
	// diskLoadHigh implies shaping and hold steady.
	diskLoadHigh
	// diskLoadOverload implies shaping and multiplicative decrease.
	diskLoadOverload
)

type diskLoadWatcher struct {
	lastInterval IntervalDiskLoadInfo
	lastUtil     float64
	loadLevel    diskLoadLevel
}

// IntervalDiskLoadInfo provides disk stats over an adjustmentInterval.
type IntervalDiskLoadInfo struct {
	// Bytes/s
	ReadBandwidth        int64
	WriteBandwidth       int64
	ProvisionedBandwidth int64
	SyncLatencyMicros    *hdrhistogram.Histogram
}

// setIntervalInfo is called at the same time as ioLoadListener.pebbleMetricsTick.
func (d *diskLoadWatcher) setIntervalInfo(load IntervalDiskLoadInfo) {
	d.lastInterval = load
	util := float64(load.ReadBandwidth+load.WriteBandwidth) / float64(load.ProvisionedBandwidth)
	log.Infof(context.Background(), "diskLoadWatcher: rb: %s, wb: %s, pb: %s, util: %.2f",
		humanizeutil.IBytes(load.ReadBandwidth), humanizeutil.IBytes(load.WriteBandwidth),
		humanizeutil.IBytes(load.ProvisionedBandwidth), util)
	// These constants are arbitrary and subject to tuning based on experiments.
	if util < 0.3 {
		// Were at moderate or lower and have not increased significantly and the
		// lastUtil was also low, then we can afford to go unlimited. We are
		// trying to be really careful to narrow this case since going unlimited
		// can blow up bandwidth.
		if d.loadLevel <= diskLoadModerate && util < d.lastUtil+0.05 && d.lastUtil < 0.3 {
			d.loadLevel = diskLoadLow
		} else {
			// util is increasing, or we just dropped from something higher than
			// moderate. Give it more time at moderate.
			d.loadLevel = diskLoadModerate
		}
	} else if util < 0.7 {
		// Wide band from [0.3,0.7) where we can gradually increase. Also 0.7 is
		// deliberately a lowish fraction since effect on compaction can be laggy
		// and kick in later. We are ok with accepting a low utilization for
		// elastic traffic to make progress.
		d.loadLevel = diskLoadModerate
	} else if util < 0.95 || (util < 2 && util < d.lastUtil-0.05) {
		// Wide band from [0.7,0.95) where we will hold. Don't want to overreact
		// and decrease too early since compaction bw usage can be lumpy. Also, if
		// we are trending downward, want to hold. Note that util < 2 will always
		// be true in real situations where one cannot actually exceed provisioned
		// bw -- but we do also run experiments where we artifically constrain the
		// provisioned bw, where this is useful.
		d.loadLevel = diskLoadHigh
	} else {
		d.loadLevel = diskLoadOverload
	}
	d.lastUtil = util
	// TODO(sumeer): Use history of syncLatencyMicros and that in the current
	// interval to bump up the load level computed earlier based on bandwidth.
	// Don't rely fully on syncLatencyMicros since (a) sync latency could arise
	// due to an external unrelated outage, (b) some customers may set sync to a
	// noop. We could also consider looking at fluctuations of peak-rate that
	// the WAL writer can sustain.
}

func (d *diskLoadWatcher) getDiskLoad() (level diskLoadLevel, unusedBandwidth int64) {
	return d.loadLevel,
		d.lastInterval.ProvisionedBandwidth - d.lastInterval.ReadBandwidth - d.lastInterval.WriteBandwidth
}

// IntervalCompactionInfo provides stats over an adjustmentInterval.
type IntervalCompactionInfo struct {
	// Both these stats must include ongoing compactions, since we desire
	// accuracy despite the occasional long-running compaction.

	// The time weighted number of concurrent running compactions.
	WeightedNumConcurrentCompactions float64
	// The bytes written by these compactions.
	CompactionWriteBytes int64
}

type compactionLimiter struct {
	lastInterval IntervalCompactionInfo
	// TODO: currently IntervalCompactionInfo is always 0, so the following will
	// also be 0. The initial experimental evaluation does not care about
	// dynamically adjusting number of compactions.
	smoothedNumConcurrentCompactions    float64
	smoothedWriteBytesPerCompactionSlot float64
	mu                                  struct {
		syncutil.Mutex
		compactionSlots int
		usedSlots       int
	}
}

func (c *compactionLimiter) setIntervalInfo(intervalInfo IntervalCompactionInfo) {
	c.lastInterval = intervalInfo
	const alpha = 0.5
	c.smoothedNumConcurrentCompactions = alpha*intervalInfo.WeightedNumConcurrentCompactions +
		(1-alpha)*c.smoothedNumConcurrentCompactions
	if intervalInfo.WeightedNumConcurrentCompactions > 0 {
		bytesPerCompactionSlot :=
			float64(intervalInfo.CompactionWriteBytes) / intervalInfo.WeightedNumConcurrentCompactions
		c.smoothedWriteBytesPerCompactionSlot = alpha*bytesPerCompactionSlot +
			(1-alpha)*c.smoothedWriteBytesPerCompactionSlot
	}
}

func (c *compactionLimiter) getCompactionSlot(force bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if force || c.mu.usedSlots < c.mu.compactionSlots {
		c.mu.usedSlots++
		return true
	}
	return false
}

func (c *compactionLimiter) returnCompactionSlot() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.usedSlots--
}

func (c *compactionLimiter) tryIncreaseSlots() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastInterval.WeightedNumConcurrentCompactions > float64(c.mu.compactionSlots)-1 {
		c.mu.compactionSlots++
		log.Infof(context.Background(), "compactionLimiter slots: %d", c.mu.compactionSlots)
		return true
	}
	return false
}

func (c *compactionLimiter) decreaseSlots() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastInterval.WeightedNumConcurrentCompactions > float64(c.mu.compactionSlots) {
		// Previous decrease has not taken effect yet. No point decreasing in a
		// multiplicative manner.
		if c.mu.compactionSlots > 0 {
			c.mu.compactionSlots--
			log.Infof(context.Background(), "compactionLimiter slots: %d", c.mu.compactionSlots)
		}
		return
	}
	c.mu.compactionSlots /= 2
}

func (c *compactionLimiter) setUnlimitedSlots() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.compactionSlots != math.MaxInt {
		log.Infof(context.Background(), "compactionLimiter slots: unlimited")
	}
	c.mu.compactionSlots = math.MaxInt
}

type intervalLSMInfo struct {
	// Flushed bytes + Ingested bytes seen by the LSM. Ingested bytes also incur
	// the cost of writing a sstable, even though that is done outside Pebble.
	// Ingested bytes don't cause WAL writes, but we ignore that difference for
	// simplicity.
	incomingBytes int64
	// regularTokensUsed and elasticTokensUsed are a combination of estimated
	// and accounted bytes for regular and elastic traffic respectively. Each of
	// these includes both writes that will get flushed and ingested bytes. But
	// regularTokensUsed+elasticTokensUsed does not need to sum up to
	// incomingBytes since they may be produced using a different method.
	regularTokensUsed int64
	elasticTokensUsed int64
}

type diskBandwidthLimiter struct {
	diskLoadWatcher   diskLoadWatcher
	compactionLimiter compactionLimiter

	lastInterval            intervalLSMInfo
	smoothedIncomingBytes   float64
	smoothedElasticFraction float64
	elasticTokens           int64
}

// Called every adjustmentInterval.
func (d *diskBandwidthLimiter) adjust(
	id IntervalDiskLoadInfo, ic IntervalCompactionInfo, il intervalLSMInfo,
) (elasticTokens int64) {
	d.diskLoadWatcher.setIntervalInfo(id)
	d.compactionLimiter.setIntervalInfo(ic)

	d.lastInterval = il
	const alpha = 0.5
	d.smoothedIncomingBytes = alpha*float64(il.incomingBytes) + (1-alpha)*d.smoothedIncomingBytes
	var intElasticFraction float64
	if il.regularTokensUsed+il.elasticTokensUsed > 0 {
		intElasticFraction =
			float64(il.elasticTokensUsed) / float64(il.regularTokensUsed+il.elasticTokensUsed)
		d.smoothedElasticFraction = alpha*intElasticFraction + (1-alpha)*d.smoothedElasticFraction
	}
	intElasticBytes := int64(float64(il.incomingBytes) * intElasticFraction)

	ll, unusedBW := d.diskLoadWatcher.getDiskLoad()
	// Compaction bw suddenly drops close to 0, from a very high value. We have
	// reduced elastic tokens close to 0. Now we need to ramp up. The problem here
	// is that by halving under overload (which we can slip into), we don't manage
	// to reduce to High due to the lag effect. So we decrease too much. So now
	// we need to increase fast.
	// Should we look at the contribution of compaction writes on overall bw and
	// be forecasting the future based on compaction backlog? This is getting
	// too complicated. In practice, we will have a mix of elastic and regular
	// traffic, though maybe regular traffic will be quite low.
	switch ll {
	case diskLoadLow:
		d.elasticTokens = math.MaxInt64
		d.compactionLimiter.setUnlimitedSlots()
		log.Infof(context.Background(), "diskBandwidthLimiter: low")
	case diskLoadModerate:
		// First try to increase compactions in case compactions are falling
		// behind.
		increased := d.compactionLimiter.tryIncreaseSlots()
		tokensFullyUtilized := func() bool {
			return il.elasticTokensUsed+1<<10 >= d.elasticTokens || d.elasticTokens == math.MaxInt64 ||
				(d.elasticTokens > 0 && float64(il.elasticTokensUsed)/float64(d.elasticTokens) >= 0.8)
		}
		if !increased && tokensFullyUtilized() {
			// Smoothed elastic bytes plus 10% of smoothedIncomingBytes is given to
			// elastic work.
			elasticBytes := (d.smoothedElasticFraction + 0.1) * d.smoothedIncomingBytes

			// Sometimes we see the tokens not increasing even though we are staying
			// for multiple intervals at moderate. This is because the smoothing can
			// have a lagging effect. We do want to increase tokens since we know
			// there is spare capacity, so we try many ways (that don't look at
			// smoothed numbers only). Also, we sometimes come here due to an
			// overload=>moderate transition because compaction bw usage can be
			// lumpy (high when there is a backlog and then dropping severely) -- in
			// that case we want to start increasing immediately, since we've likely
			// decreased too much.
			intBasedElasticTokens := (d.smoothedElasticFraction + 0.1) * float64(il.incomingBytes)
			if elasticBytes < intBasedElasticTokens {
				elasticBytes = intBasedElasticTokens
			}
			if elasticBytes < 1.1*float64(il.elasticTokensUsed) {
				elasticBytes = 1.1 * float64(il.elasticTokensUsed)
			}
			d.elasticTokens = int64(elasticBytes)
			if d.elasticTokens == 0 {
				// Don't get stuck in a situation where smoothedIncomingBytes are 0.
				d.elasticTokens = math.MaxInt64
			}
			log.Infof(context.Background(),
				"diskBandwidthLimiter: moderate fr: %.2f, smoothed-incoming: %s, unusedBW: %s, elasticBytes/Tokens: %s",
				d.smoothedElasticFraction, humanizeutil.IBytes(int64(d.smoothedIncomingBytes)),
				humanizeutil.IBytes(unusedBW),
				humanizeutil.IBytes(int64(elasticBytes)))
		} else {
			log.Infof(context.Background(), "diskBandwidthLimiter: moderate elasticTokens (limit, used): %d, %d",
				d.elasticTokens, il.elasticTokensUsed)
		}
	case diskLoadHigh:
		if float64(intElasticBytes) >= d.compactionLimiter.smoothedWriteBytesPerCompactionSlot {
			// Try to decrease elastic tokens and increase compactions. This is a
			// very rough heuristic since compactions also incur reads, which are
			// not being accounted for here, but some compensation is better than
			// nothing.
			if d.compactionLimiter.tryIncreaseSlots() {
				// TODO: this will never happen in our experiments.
				d.elasticTokens = intElasticBytes -
					int64(d.compactionLimiter.smoothedWriteBytesPerCompactionSlot)
			}
		}
		log.Infof(context.Background(), "diskBandwidthLimiter: high elastic fr: %.2f, smoothed-incoming: %d, elasticTokens: %d",
			d.smoothedElasticFraction, int64(d.smoothedIncomingBytes), d.elasticTokens)
	case diskLoadOverload:
		d.compactionLimiter.decreaseSlots()
		// Sometimes we come here after a low => overload transition. The
		// intElasticBytes will be very high because tokens were unlimited. We
		// don't want to use that as the starting point of the decrease if the
		// smoothed value is lower. Hence, the min logic below, to try to dampen
		// the increase quickly.
		d.elasticTokens = intElasticBytes / 2
		elasticBytes := int64(d.smoothedElasticFraction * d.smoothedIncomingBytes)
		if elasticBytes/2 < d.elasticTokens {
			d.elasticTokens = elasticBytes / 2
		}
		log.Infof(context.Background(), "diskBandwidthLimiter: overload %s",
			humanizeutil.IBytes(d.elasticTokens))
	}
	// We can end up with 0 elastic tokens here -- e.g. if intElasticBytes was 0
	// but we were still overloaded because of compactions. The trouble with 0
	// elastic tokens is that if we don't admit anything, we cannot correct on
	// occasional poor estimate of the per-request bytes. So we decide to give
	// out at least 1 token. A single elastic request should not be too big for
	// this to matter.
	// 60 is a hack here since we know we give out tokens every 250ms for 15s.
	// We want to give out one token in each tick since if we get unlucky and
	// give out 1 in the first tick and that does not get used the subsequent
	// ticks will reduce the tokens to 0.
	d.elasticTokens = max(60, d.elasticTokens)
	return d.elasticTokens
}
