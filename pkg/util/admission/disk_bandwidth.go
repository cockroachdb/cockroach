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
	"math"

	"github.com/HdrHistogram/hdrhistogram-go"
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
	// These constants are arbitrary and subject to tuning based on experiments.
	if util < 0.5 {
		if d.loadLevel <= diskLoadModerate {
			// Two consecutive intervals with load <= diskLoadModerate
			d.loadLevel = diskLoadLow
		} else {
			d.loadLevel = diskLoadModerate
		}
	} else if util < 0.8 {
		if d.loadLevel <= diskLoadHigh {
			d.loadLevel = diskLoadModerate
		} else {
			d.loadLevel = diskLoadHigh
		}
	} else if util < 0.95 {
		d.loadLevel = diskLoadHigh
	} else {
		d.loadLevel = diskLoadOverload
	}
	// TODO(sumeer): Use history of syncLatencyMicros and that in the current
	// interval to bump up the load level computed earlier based on bandwidth.
	// Don't rely fully on syncLatencyMicros since (a) sync latency could arise
	// due to an external unrelated outage, (b) some customers may set sync to a
	// noop. We could also consider looking at fluctuations of peak-rate that
	// the WAL writer can sustain.
}

func (d *diskLoadWatcher) getDiskLoad() diskLoadLevel {
	return d.loadLevel
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
	lastInterval                        IntervalCompactionInfo
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
		}
		return
	}
	c.mu.compactionSlots /= 2
}

func (c *compactionLimiter) setUnlimitedSlots() {
	c.mu.Lock()
	defer c.mu.Unlock()
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

	ll := d.diskLoadWatcher.getDiskLoad()
	switch ll {
	case diskLoadLow:
		d.elasticTokens = math.MaxInt64
		d.compactionLimiter.setUnlimitedSlots()
	case diskLoadModerate:
		// First try to increase compactions in case compactions are falling
		// behind.
		increased := d.compactionLimiter.tryIncreaseSlots()
		if !increased && il.elasticTokensUsed >= d.elasticTokens {
			// Smoothed elastic bytes.
			elasticBytes := d.smoothedElasticFraction * d.smoothedIncomingBytes
			// Increase smoothedIncomingBytes by 10% and all of that goes to elastic
			// work.
			elasticBytes += 0.1 * d.smoothedIncomingBytes
			d.elasticTokens = int64(elasticBytes)
			if d.elasticTokens == 0 {
				// Don't get stuck in a situation where smoothedIncomingBytes are 0.
				d.elasticTokens = math.MaxInt64
			}
		}
	case diskLoadHigh:
		if float64(intElasticBytes) >= d.compactionLimiter.smoothedWriteBytesPerCompactionSlot {
			// Try to decrease elastic tokens and increase compactions. This is a
			// very rough heuristic since compactions also incur reads, which are
			// not being accounted for here, but some compensation is better than
			// nothing.
			if d.compactionLimiter.tryIncreaseSlots() {
				d.elasticTokens = intElasticBytes -
					int64(d.compactionLimiter.smoothedNumConcurrentCompactions)
			}
		}
	case diskLoadOverload:
		d.compactionLimiter.decreaseSlots()
		d.elasticTokens = intElasticBytes / 2
	}
	return d.elasticTokens
}
