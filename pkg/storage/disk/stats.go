// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package disk

import (
	"fmt"
	"time"
)

// SectorSizeBytes is the number of bytes stored on a disk sector.
const SectorSizeBytes = 512

// Stats describes statistics for an individual disk or volume.
type Stats struct {
	// ReadsCount is the count of read operations completed successfully.
	ReadsCount int
	// ReadsMerged is the count of adjacent read operations merged by the
	// operating system before ultimately handling the I/O request to the disk.
	ReadsMerged int
	// ReadsSectors is the count of sectors read successfully.
	ReadsSectors int
	// ReadsDuration is the total time spent by all reads.
	//
	// On Linux this is measured as the time spent between
	// blk_mq_alloc_request() and __blk_mq_end_request().
	ReadsDuration time.Duration
	// WritesCount is the count of write operations completed successfully.
	WritesCount int
	// WritesMerged is the count of adjacent write operations merged by the
	// operating system before ultimately handing the I/O request to the disk.
	WritesMerged int
	// WritesSectors is the count of sectors written successfully.
	WritesSectors int
	// WritesDuration is the total time spent by all writes.
	//
	// On Linux this is measured as the time spent between
	// blk_mq_alloc_request() and __blk_mq_end_request().
	WritesDuration time.Duration
	// InProgressCount is the count of I/O operations currently in-progress.
	InProgressCount int
	// CumulativeDuration is the total time spent doing I/O.
	CumulativeDuration time.Duration
	// WeightedIODuration is a weighted measure of cumulative time spent in IO.
	//
	// On Linux, this field is incremented at each I/O start, I/O completion,
	// I/O merge, or read of these stats from /proc, by the number of I/Os in
	// progress (InProgressCount) times the duration spent doing I/O since the
	// last update of this field. This can provide an easy measure of both I/O
	// completion time and the backlog that may be accumulating.
	WeightedIODuration time.Duration
	// DiscardsCount is the count of discard operations completed successfully.
	DiscardsCount int
	// DiscardsMerged is the count of adjacent discard operations merged by the
	// operating system before ultimately handing the I/O request to disk.
	DiscardsMerged int
	// DiscardsSectors is the count of sectors discarded successfully.
	DiscardsSectors int
	// DiscardsDuration is the total time spent by all discards.
	//
	// On Linux this is measured as the time spent between
	// blk_mq_alloc_request() and __blk_mq_end_request().
	DiscardsDuration time.Duration
	// FlushesCount is the total number of flush requests completed
	// successfully.
	//
	// On Linux, the block layer combines flush requests and executes at most
	// one at a time. This counts flush requests executed by disk. It is not
	// available for partitions.
	FlushesCount int
	// FlushesDuration is the total time spent by all flush requests.
	FlushesDuration time.Duration
}

// String implements fmt.Stringer.
func (s *Stats) String() string {
	return fmt.Sprintf("r: (%d, %d, %d, %s), w: (%d, %d, %d, %s), (now: %d, c: %s, w: %s), d: (%d, %d, %d, %s), f: (%d, %s)",
		s.ReadsCount, s.ReadsMerged, s.ReadsSectors, s.ReadsDuration,
		s.WritesCount, s.WritesMerged, s.WritesSectors, s.WritesDuration,
		s.InProgressCount, s.CumulativeDuration, s.WeightedIODuration,
		s.DiscardsCount, s.DiscardsMerged, s.DiscardsSectors, s.DiscardsDuration,
		s.FlushesCount, s.FlushesDuration)
}

// BytesRead computes the total number of bytes read from disk.
func (s *Stats) BytesRead() int {
	return s.ReadsSectors * SectorSizeBytes
}

// BytesWritten computes the total number of bytes written to disk.
func (s *Stats) BytesWritten() int {
	return s.WritesSectors * SectorSizeBytes
}
