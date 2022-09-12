// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package admissionpb

import (
	"math"

	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// Score returns, as the second return value, whether IO admission control is
// considering the Store overloaded wrt compaction of L0. The first return
// value is a 1-normalized float (i.e. 1.0 is the threshold at which the
// second value flips to true).
//
// The zero value returns (0, false). Use of the nil pointer is not allowed.
//
// TODO(sumeer): consider whether we need to enhance this to incorporate
// overloading via flush bandwidth. I suspect we can get away without
// incorporating flush bandwidth since typically chronic overload will be due
// to compactions falling behind (though that may change if we increase the
// max number of compactions). And we will need to incorporate overload due to
// disk bandwidth bottleneck.
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
	f := math.Max(
		float64(iot.L0NumFiles)/float64(iot.L0NumFilesThreshold),
		float64(iot.L0NumSubLevels)/float64(iot.L0NumSubLevelsThreshold),
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
