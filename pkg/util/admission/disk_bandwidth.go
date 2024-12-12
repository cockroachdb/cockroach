// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
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
// - We estimate the write amplification using a model of the incoming 	 writes
//   and actual bytes written to disk in the previous interval. We then use this
//   model when deducting tokens for disk writes.
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

// intervalDiskLoadInfo provides disk stats over an adjustmentInterval.
type intervalDiskLoadInfo struct {
	// intReadBytes represents measured disk read bytes in a given interval.
	intReadBytes int64
	// intWriteBytes represents measured write bytes in a given interval.
	intWriteBytes int64
	// intProvisionedDiskBytes represents the disk writes (in bytes) available in
	// an adjustmentInterval.
	intProvisionedDiskBytes int64
	// elasticBandwidthMaxUtil sets the maximum disk bandwidth utilization for
	// elastic requests
	elasticBandwidthMaxUtil float64
}

// diskBandwidthLimiterState is used as auxiliary information for logging
// purposes and keeping past state.
type diskBandwidthLimiterState struct {
	tokens     diskTokens
	prevTokens diskTokens
	usedTokens [admissionpb.NumStoreWorkTypes]diskTokens
	diskBWUtil float64
	diskLoad   intervalDiskLoadInfo
}

// diskBandwidthLimiter produces tokens for elastic work.
type diskBandwidthLimiter struct {
	state diskBandwidthLimiterState
}

func newDiskBandwidthLimiter() *diskBandwidthLimiter {
	return &diskBandwidthLimiter{
		state: diskBandwidthLimiterState{},
	}
}

// diskTokens tokens represent actual bytes and IO on physical disks. Currently,
// these are used to impose disk bandwidth limits on elastic traffic, but
// regular traffic will also deduct from these buckets.
type diskTokens struct {
	readByteTokens  int64
	writeByteTokens int64
	readIOPSTokens  int64
	writeIOPSTokens int64
}

// computeElasticTokens is called every adjustmentInterval.
func (d *diskBandwidthLimiter) computeElasticTokens(
	id intervalDiskLoadInfo, usedTokens [admissionpb.NumStoreWorkTypes]diskTokens,
) diskTokens {
	// TODO(aaditya): Include calculation for read and IOPS.
	// Issue: https://github.com/cockroachdb/cockroach/issues/107623

	// We are using disk read bytes over the previous adjustment interval as a
	// proxy for future reads. This is a bad estimate, but we account for errors
	// in this estimate separately at a higher frequency. See
	// kvStoreTokenGranter.adjustDiskTokenErrorLocked.
	const alpha = 0.5
	smoothedReadBytes := alpha*float64(id.intReadBytes) + alpha*float64(d.state.diskLoad.intReadBytes)
	// Pessimistic approach using the max value between the smoothed and current
	// reads.
	intReadBytes := int64(math.Max(smoothedReadBytes, float64(id.intReadBytes)))
	intReadBytes = int64(math.Max(0, float64(intReadBytes)))
	diskWriteTokens := int64(float64(id.intProvisionedDiskBytes)*id.elasticBandwidthMaxUtil) - intReadBytes
	// TODO(aaditya): consider setting a different floor to avoid starving out
	// elastic writes completely due to out-sized reads from above.
	diskWriteTokens = int64(math.Max(0, float64(diskWriteTokens)))

	totalUsedTokens := sumDiskTokens(usedTokens)
	tokens := diskTokens{
		readByteTokens:  intReadBytes,
		writeByteTokens: diskWriteTokens,
		readIOPSTokens:  0,
		writeIOPSTokens: 0,
	}
	prevState := d.state
	d.state = diskBandwidthLimiterState{
		tokens:     tokens,
		prevTokens: prevState.tokens,
		usedTokens: usedTokens,
		diskBWUtil: float64(totalUsedTokens.writeByteTokens) / float64(prevState.tokens.writeByteTokens),
		diskLoad:   id,
	}
	return tokens
}

func (d *diskBandwidthLimiter) SafeFormat(p redact.SafePrinter, _ rune) {
	ib := humanizeutil.IBytes
	p.Printf("diskBandwidthLimiter (tokenUtilization %.2f, tokensUsed (elastic %s, "+
		"snapshot %s, regular %s) tokens (write %s (prev %s), read %s (prev %s)), writeBW %s/s, "+
		"readBW %s/s, provisioned %s/s)",
		d.state.diskBWUtil,
		ib(d.state.usedTokens[admissionpb.ElasticStoreWorkType].writeByteTokens),
		ib(d.state.usedTokens[admissionpb.SnapshotIngestStoreWorkType].writeByteTokens),
		ib(d.state.usedTokens[admissionpb.RegularStoreWorkType].writeByteTokens),
		ib(d.state.tokens.writeByteTokens),
		ib(d.state.prevTokens.writeByteTokens),
		ib(d.state.tokens.readByteTokens),
		ib(d.state.prevTokens.readByteTokens),
		ib(d.state.diskLoad.intWriteBytes/adjustmentInterval),
		ib(d.state.diskLoad.intReadBytes/adjustmentInterval),
		ib(d.state.diskLoad.intProvisionedDiskBytes/adjustmentInterval),
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
		sumTokens.readIOPSTokens += tokens[i].readIOPSTokens
		sumTokens.writeIOPSTokens += tokens[i].writeIOPSTokens
	}
	return sumTokens
}
