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
	// elasticBandwidthMaxUtil sets the maximum disk bandwidth utilization for
	// elastic requests
	// TODO(aaditya): hook this into the cluster setting
	elasticBandwidthMaxUtil float64
}

// intervalUtilInfo provides stats about the diskTokens utilization over an
// adjustmentInterval.
type intervalUtilInfo struct {
	actualTokensUsed [admissionpb.NumWorkClasses]diskTokens
	requestedTokens  [admissionpb.NumWorkClasses]diskTokens
}

type diskBandwidthLimiterState struct {
	estimatedWriteAmp float64
	tokens            diskTokens
	usedTokens        [admissionpb.NumWorkClasses]diskTokens
	diskBWUtil        float64
	diskLoad          intervalDiskLoadInfo
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

type diskTokens struct {
	readBWTokens    int64
	writeBWTokens   int64
	readIOPSTokens  int64
	writeIOPSTokens int64
}

// computeElasticTokens is called every adjustmentInterval.
func (d *diskBandwidthLimiter) computeElasticTokens(
	id intervalDiskLoadInfo, il intervalUtilInfo,
) diskTokens {
	// TODO(aaditya): Include calculation for read and IOPS.
	// Issue: https://github.com/cockroachdb/cockroach/issues/107623
	writeBWTokens := int64(float64(id.provisionedBandwidth)*id.elasticBandwidthMaxUtil) - id.readBandwidth
	// TODO(aaditya): consider setting a different floor to avoid startving out
	// elastic writes completely.
	writeBWTokens = int64(math.Max(0, float64(writeBWTokens)))

	prevState := d.state
	const alpha = 0.5
	diskLoad := id
	requestedTokens := sumDiskTokens(il.requestedTokens[admissionpb.ElasticWorkClass], il.requestedTokens[admissionpb.RegularWorkClass])
	usedTokens := sumDiskTokens(il.actualTokensUsed[admissionpb.ElasticWorkClass], il.actualTokensUsed[admissionpb.RegularWorkClass])
	writeAmp := float64(diskLoad.writeBandwidth) / float64(requestedTokens.writeBWTokens)
	smoothedWriteAmp := prevState.estimatedWriteAmp*alpha + writeAmp*alpha
	// NB: There are times when write-amp can be less than 1 (e.g. during the
	// very first iteration).
	smoothedWriteAmp = math.Max(1.0, smoothedWriteAmp)

	tokens := diskTokens{
		readBWTokens:    0,
		writeBWTokens:   writeBWTokens,
		readIOPSTokens:  0,
		writeIOPSTokens: 0,
	}
	d.state.estimatedWriteAmp = smoothedWriteAmp
	d.state.diskBWUtil = float64(usedTokens.writeBWTokens) / float64(writeBWTokens)
	d.state.tokens = tokens
	d.state.usedTokens = il.actualTokensUsed
	d.state.diskLoad = diskLoad
	return tokens
}

func (d *diskBandwidthLimiter) SafeFormat(p redact.SafePrinter, _ rune) {
	ib := humanizeutil.IBytes
	p.Printf("diskBandwidthLimiter (tokenUtilization %.2f, tokensUsed (elastic %s, "+
		"regular %s) tokens (write %s), estimatedWriteAmp %.2f, writeBW %s/s, readBW %s/s, provisioned %s/s)",
		d.state.diskBWUtil,
		ib(d.state.usedTokens[admissionpb.ElasticWorkClass].writeBWTokens),
		ib(d.state.usedTokens[admissionpb.RegularWorkClass].writeBWTokens),
		ib(d.state.tokens.writeBWTokens),
		d.state.estimatedWriteAmp,
		ib(d.state.diskLoad.writeBandwidth),
		ib(d.state.diskLoad.readBandwidth),
		ib(d.state.diskLoad.provisionedBandwidth),
	)
}

func (d *diskBandwidthLimiter) String() string {
	return redact.StringWithoutMarkers(d)
}

func sumDiskTokens(l diskTokens, r diskTokens) diskTokens {
	return diskTokens{
		readBWTokens:    l.readBWTokens + r.readBWTokens,
		writeBWTokens:   l.writeBWTokens + r.writeBWTokens,
		readIOPSTokens:  l.readBWTokens + r.readIOPSTokens,
		writeIOPSTokens: l.writeIOPSTokens + r.writeIOPSTokens,
	}
}
