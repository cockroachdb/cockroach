// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/redact"
)

// TODO(aaditya): Update this comment once read and IOPS are integrated to this.
// Issue: https://github.com/cockroachdb/cockroach/issues/107623
//
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
// Due to these challenges, we adopt a goal of simplicity of design.
//
// - The disk bandwidth limiter estimates disk write byte tokens using the
//   provisioned value and subtracts away reads seen in the previous interval.
//
// - We estimate the write amplification using a model of the incoming writes
//   and actual bytes written to disk in the previous interval. We then use this
//   model when deducting tokens for disk writes.
//
// - We try to keep some headroom in the disk bandwidth utilization since the
//   control mechanism is coarse. Specifically, see the cluster setting
//   kvadmission.store.elastic_disk_bandwidth_max_util. Our primary goal is
//   for the mean usage to be close this goal utilization, with the secondary
//   goal to avoid exceeding by a huge amount. We have had implementations
//   that significantly undershoot the goal (see
//   https://github.com/cockroachdb/cockroach/issues/160964), which is
//   undesirable as it is wasteful and causes users to turn off this
//   functionality.
//
// Since we are using a simple approach that is somewhat coarse in its
// behavior, we start by limiting its application to incoming writes that are
// deemed "elastic". In practice this is done by implying a work-class from
// the priority (e.g. priorities < NormalPri are deemed elastic).
// Additionally, incoming range snapshots writing to disk can also be limited
// using this mechanism.
//
// Extending this to all incoming writes is future work.

// intervalDiskLoadInfo provides disk stats over an adjustmentInterval.
type intervalDiskLoadInfo struct {
	// intReadBytes represents measured disk read bytes in a given interval.
	intReadBytes int64
	// intWriteBytes represents measured write bytes in a given interval.
	intWriteBytes int64
	// intProvisionedDiskBytes represents the combined disk reads + writes (in
	// bytes) available in an adjustmentInterval.
	intProvisionedDiskBytes int64
	// elasticBandwidthMaxUtil sets the maximum disk bandwidth utilization. It
	// guides disk write bandwidth token allocation that is used to shape
	// elastic write requests.
	elasticBandwidthMaxUtil float64
}

// diskBandwidthLimiterState is used as auxiliary information for logging
// purposes and keeping past state.
type diskBandwidthLimiterState struct {
	// tokens represents the token count for the latest (ongoing)
	// adjustmentInterval.
	tokens diskTokens
	// prevTokens is the token count for the previous adjustmentInterval.
	prevTokens diskTokens
	// The tokens used in the previous adjustmentInterval, per work type.
	prevUsedTokens [admissionpb.NumStoreWorkTypes]diskTokens
	// prevWriteTokenUtil is the fraction of prevTokens.writeByteTokens used
	// across the sum of prevUsedTokens[i].writeByteTokens (for all work types
	// i).
	prevWriteTokenUtil float64
	prevDiskErrorStats diskErrorStats
	// prevRemainingDiskWriteTokens is the disk write token balance at the end
	// of the previous interval. A negative value indicates overadmission.
	prevRemainingDiskWriteTokens int64
	// prevDiskLoad represents the disk load info for the previous
	// adjustmentInterval.
	prevDiskLoad intervalDiskLoadInfo
}

// diskBandwidthLimiter produces tokens for elastic work.
type diskBandwidthLimiter struct {
	state diskBandwidthLimiterState
	// Only used for logging.
	unlimitedTokensOverride bool
}

func newDiskBandwidthLimiter() *diskBandwidthLimiter {
	return &diskBandwidthLimiter{
		state: diskBandwidthLimiterState{},
	}
}

// getProvisionedBandwidth returns the provisioned disk bandwidth in bytes/second.
func (d *diskBandwidthLimiter) getProvisionedBandwidth() int64 {
	return d.state.prevDiskLoad.intProvisionedDiskBytes / adjustmentInterval
}

// diskTokens represents tokens corresponding to bytes and IO on physical
// disks.
//
// Even though the provisioned limit is on combined read and write bandwidth,
// we track read and write tokens separately, since it allows us to reserve
// read tokens based on activity in the previous adjustment interval.
type diskTokens struct {
	readByteTokens  int64
	writeByteTokens int64

	// TODO(sumeer): add tokens corresponding to IOPS
	// https://github.com/cockroachdb/cockroach/issues/107623).
	//
	// readIOPSTokens int64
	// writeIOPSTokens int64
}

// computeElasticTokens is called every adjustmentInterval, and computes state
// for the next adjustmentInterval. It returns the disk tokens to allocate
// over the next adjustmentInterval.
func (d *diskBandwidthLimiter) computeElasticTokens(
	id intervalDiskLoadInfo,
	usedTokens [admissionpb.NumStoreWorkTypes]diskTokens,
	diskErrStats diskErrorStats,
	remainingDiskWriteTokens int64,
) diskTokens {
	// TODO(aaditya): Include calculation for read and IOPS.
	// Issue: https://github.com/cockroachdb/cockroach/issues/107623

	// We use disk read bytes over the previous adjustment interval as a proxy
	// for future reads. This is a conservative choice that allows us to set
	// aside tokens for reads up front, rather than only react when we observe
	// the read (which could be late, if reads happen after writes have consumed
	// all the available tokens). Admittedly, this estimate can be poor, but we
	// account for errors in this estimate separately at a higher frequency (1s
	// intervals), in kvStoreTokenGranter.adjustDiskTokenErrorLocked.
	const alpha = 0.5
	smoothedReadBytes := alpha*float64(id.intReadBytes) + alpha*float64(d.state.prevDiskLoad.intReadBytes)
	// Pessimistic approach using the max value between the smoothed and current
	// reads.
	intReadBytes := int64(math.Max(smoothedReadBytes, float64(id.intReadBytes)))
	intReadBytes = int64(math.Max(0, float64(intReadBytes)))
	// Write tokens are simply the goal utilization times the provisioned bytes
	// with the bytes set aside for reads subtracted.
	diskWriteTokens := int64(float64(id.intProvisionedDiskBytes)*id.elasticBandwidthMaxUtil) - intReadBytes
	// TODO(aaditya): consider setting a different floor to avoid starving out
	// elastic writes completely due to out-sized reads from above.
	diskWriteTokens = int64(math.Max(0, float64(diskWriteTokens)))

	totalUsedTokens := sumDiskTokens(usedTokens)
	tokens := diskTokens{
		readByteTokens:  intReadBytes,
		writeByteTokens: diskWriteTokens,
	}
	prevState := d.state
	writeTokenUtil := 0.0
	if prevState.tokens.writeByteTokens > 0 {
		writeTokenUtil = float64(totalUsedTokens.writeByteTokens) / float64(prevState.tokens.writeByteTokens)
	}
	d.state = diskBandwidthLimiterState{
		tokens:                       tokens,
		prevTokens:                   prevState.tokens,
		prevUsedTokens:               usedTokens,
		prevWriteTokenUtil:           writeTokenUtil,
		prevDiskErrorStats:           diskErrStats,
		prevRemainingDiskWriteTokens: remainingDiskWriteTokens,
		prevDiskLoad:                 id,
	}
	return tokens
}

func (d *diskBandwidthLimiter) SafeFormat(p redact.SafePrinter, _ rune) {
	ib := humanizeutil.IBytes
	var unlimitedPrefix string
	if d.unlimitedTokensOverride {
		unlimitedPrefix = " (unlimited)"
	}

	var overadmissionStr string
	if d.state.prevRemainingDiskWriteTokens < 0 {
		overadmissionStr = fmt.Sprintf(" overadmission %s/s",
			ib(-d.state.prevRemainingDiskWriteTokens/adjustmentInterval))
	}

	p.Printf("diskBandwidthLimiter%s (writeUtil %.2f, tokensUsed (elastic %s, "+
		"snapshot %s, regular %s) tokens (write %s (prev %s), read %s (prev %s)), writeBW %s/s, "+
		"readBW %s/s, provisioned %s/s, err(cum,abs) write: %s,%s read: %s,%s)%s",
		redact.SafeString(unlimitedPrefix),
		d.state.prevWriteTokenUtil,
		ib(d.state.prevUsedTokens[admissionpb.ElasticStoreWorkType].writeByteTokens),
		ib(d.state.prevUsedTokens[admissionpb.SnapshotIngestStoreWorkType].writeByteTokens),
		ib(d.state.prevUsedTokens[admissionpb.RegularStoreWorkType].writeByteTokens),
		ib(d.state.tokens.writeByteTokens),
		ib(d.state.prevTokens.writeByteTokens),
		ib(d.state.tokens.readByteTokens),
		ib(d.state.prevTokens.readByteTokens),
		ib(d.state.prevDiskLoad.intWriteBytes/adjustmentInterval),
		ib(d.state.prevDiskLoad.intReadBytes/adjustmentInterval),
		ib(d.state.prevDiskLoad.intProvisionedDiskBytes/adjustmentInterval),
		ib(d.state.prevDiskErrorStats.cumError.writeByteTokens),
		ib(d.state.prevDiskErrorStats.absError.writeByteTokens),
		ib(d.state.prevDiskErrorStats.cumError.readByteTokens),
		ib(d.state.prevDiskErrorStats.absError.readByteTokens),
		redact.SafeString(overadmissionStr),
	)
}

func (d *diskBandwidthLimiter) String() string {
	return redact.StringWithoutMarkers(d)
}

func sumDiskTokens(tokens [admissionpb.NumStoreWorkTypes]diskTokens) diskTokens {
	var sumTokens diskTokens
	for i := 0; i < admissionpb.NumStoreWorkTypes; i++ {
		sumTokens.readByteTokens += tokens[i].readByteTokens
		sumTokens.writeByteTokens += tokens[i].writeByteTokens
	}
	return sumTokens
}
