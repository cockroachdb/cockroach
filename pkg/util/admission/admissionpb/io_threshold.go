// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admissionpb

import (
	"math"

	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// Score returns, as the second return value, whether IO admission control is
// considering the Store overloaded wrt compaction of L0. The first return
// value is a 1-normalized float, where 1.0 represents severe overload, and
// therefore 1.0 is the threshold at which the second value flips to true.
// Admission control currently trys to maintain a store around a score
// threshold of 0.5 for regular work and lower than 0.25 for elastic work. NB:
// this is an incomplete representation of the signals considered by admission
// control -- admission control additionally considers disk and flush
// throughput bottlenecks.
//
// The zero value returns (0, false). Use of the nil pointer is not allowed.
//
// NOTE: Future updates to the scoring function should be version gated as the
// threshold is gossiped and used to determine lease/replica placement via the
// allocator.
//
// IOThreshold has various parameters that can evolve over time. The source of
// truth for an IOThreshold struct is admission.ioLoadListener, and is
// propagated elsewhere using the admission.IOThresholdConsumer interface. No
// other production code should create one from scratch.
func (iot *IOThreshold) Score() (float64, bool) {
	// iot.L0NumFilesThreshold and iot.L0NumSubLevelsThreshold are initialized to
	// 0 by default, and there appears to be a period of time before we update
	// iot.L0NumFilesThreshold and iot.L0NumSubLevelsThreshold to their
	// appropriate values. During this period of time, to prevent dividing by 0
	// below and Score() returning NaN, we check if iot.L0NumFilesThreshold or
	// iot.L0NumSubLevelsThreshold are 0 (i.e. currently uninitialized) and
	// return 0 as the score if so.
	if iot == nil || iot.L0NumFilesThreshold == 0 || iot.L0NumSubLevelsThreshold == 0 {
		return 0, false
	}
	numSubLevels := iot.L0NumSubLevels
	if iot.L0MinimumSizePerSubLevel > 0 {
		// Upper-bound on number of sub-levels. See the comment for the cluster
		// setting admission.l0_sub_level_count_overload_threshold.
		maxNumSubLevels := int64(math.Round(float64(iot.L0Size) / float64(iot.L0MinimumSizePerSubLevel)))
		// Say numSubLevels is 30 and maxNumSubLevels is 5. We don't want to
		// ignore the huge disparity, since that could allow the numSubLevels to
		// grow to a huge value. So we place a lower-bound. The divisor of 3 was
		// chosen somewhat arbitrarily.
		//
		// NB: the lower-bound can be greater than the upper-bound, in which case
		// the lower-bound takes precedence.
		minNumSubLevels := numSubLevels / 3
		if numSubLevels > maxNumSubLevels {
			numSubLevels = maxNumSubLevels
		}
		if numSubLevels < minNumSubLevels {
			numSubLevels = minNumSubLevels
		}
	}
	f := math.Max(
		float64(iot.L0NumFiles)/float64(iot.L0NumFilesThreshold),
		float64(numSubLevels)/float64(iot.L0NumSubLevelsThreshold),
	)
	return f, f > 1.0
}

// SafeFormat implements redact.SafeFormatter.
func (iot *IOThreshold) SafeFormat(s interfaces.SafePrinter, _ rune) {
	if iot == nil {
		s.Printf("N/A")
		return
	}
	sc, overload := iot.Score()
	s.Printf("%.3f", redact.SafeFloat(sc))
	if overload {
		s.Printf("[L0-overload]")
	}
}

func (iot *IOThreshold) String() string {
	return redact.StringWithoutMarkers(iot)
}
