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

	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// TODO(aaditya): Update this.
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

// intervalUtilInfo provides stats about the diskTokens utilization over an
// adjustmentInterval.
type intervalUtilInfo struct {
	actualTokensUsed [admissionpb.NumWorkClasses]diskTokens
	requestedTokens  [admissionpb.NumWorkClasses]diskTokens
}

type diskBandwidthLimiterState struct {
	estimatedWriteAmp float64
}

// diskBandwidthLimiter produces tokens for elastic work.
type diskBandwidthLimiter struct {
	state diskBandwidthLimiterState
}

func makeDiskBandwidthLimiter() diskBandwidthLimiter {
	return diskBandwidthLimiter{
		state: diskBandwidthLimiterState{},
	}
}

type diskTokens struct {
	readBWTokens    int64
	writeBWTokens   int64
	readIOPSTokens  int64
	writeIOPSTokens int64
}

// computeElasticTokens is called every adjustmentInterval.
func (d *diskBandwidthLimiter) computeElasticTokens(
	ctx context.Context, id intervalDiskLoadInfo, il intervalUtilInfo,
) diskTokens {
	// TODO(aaditya): This should be a custer setting.
	maxElasticBWUtil := 0.9
	// TODO(aaditya): Is there a better way to account for reads but not really account for reads.
	writeBWTokens := int64(float64(id.provisionedBandwidth)*maxElasticBWUtil) - id.readBandwidth

	prevState := d.state
	const alpha = 0.5
	diskLoad := id
	requestedTokens := sumDiskTokens(il.requestedTokens[admissionpb.ElasticWorkClass], il.requestedTokens[admissionpb.RegularWorkClass])
	currentWriteAmp := float64(diskLoad.writeBandwidth) / float64(requestedTokens.writeBWTokens)
	if currentWriteAmp < 1.0 {
		panic(errors.AssertionFailedf("current write-amp less than 1, %f", currentWriteAmp))
	}
	smoothedWriteAmp := prevState.estimatedWriteAmp*alpha + currentWriteAmp*alpha
	d.state.estimatedWriteAmp = smoothedWriteAmp

	return diskTokens{
		readBWTokens:    0,
		writeBWTokens:   writeBWTokens,
		readIOPSTokens:  0,
		writeIOPSTokens: 0,
	}
}

func (d *diskBandwidthLimiter) SafeFormat(p redact.SafePrinter, _ rune) {
	ib := humanizeutil.IBytes
	//p.Printf("diskBandwidthLimiter %s (%v): elastic-frac: %.2f, incoming: %s, "+
	//	"elastic-tokens (used %s): %s",
	//	diskLoadLevelString(level), d.diskLoadWatcher, d.state.smoothedElasticFraction,
	//	ib(int64(d.state.smoothedIncomingBytes)), ib(d.state.prevElasticTokensUsed),
	//	ib(d.state.elasticTokens))
	p.Printf("diskBandwidthLimiter %s (%s): elastic-frac: %.2f, incoming: %s, "+
		"elastic-tokens (used %s): %s",
		"diskLoadLevelString(level)", "d.diskLoadWatcher", 0.0,
		ib(0), ib(0),
		ib(0))
}

func sumDiskTokens(l diskTokens, r diskTokens) diskTokens {
	return diskTokens{
		readBWTokens:    l.readBWTokens + r.readBWTokens,
		writeBWTokens:   l.writeBWTokens + r.writeBWTokens,
		readIOPSTokens:  l.readBWTokens + r.readIOPSTokens,
		writeIOPSTokens: l.writeIOPSTokens + r.writeIOPSTokens,
	}
}
