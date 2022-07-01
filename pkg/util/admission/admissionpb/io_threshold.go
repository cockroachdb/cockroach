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
// considering the Store overloaded. The first return value is a 1-normalized
// float (i.e. 1.0 is the threshold at which the second value flips to true).
//
// The zero value returns (0, false). Use of the nil pointer is not allowed.
func (iot IOThreshold) Score() (float64, bool) {
	if iot == (IOThreshold{}) {
		return 0, false
	}
	f := math.Max(
		float64(iot.L0NumFiles)/float64(iot.L0NumFilesThreshold),
		float64(iot.L0NumSubLevels)/float64(iot.L0NumSubLevelsThreshold),
	)
	return f, f > 1.0
}

// SafeFormat implements redact.SafeFormatter.
func (iot IOThreshold) SafeFormat(s interfaces.SafePrinter, _ rune) {
	if iot == (IOThreshold{}) {
		s.Printf("N/A")
	}
	sc, overload := iot.Score()
	s.Printf("%.3f", redact.SafeFloat(sc))
	if overload {
		s.Printf("[overload]")
	}
}

func (iot IOThreshold) String() string {
	return redact.StringWithoutMarkers(iot)
}
