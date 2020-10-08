// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"time"

	"github.com/codahale/hdrhistogram"
)

var _ periodic = &slidingHistogram{}

// A deprecatedWindowedHistogram is a wrapper around an
// hdrhistogram.WindowedHistogram. The caller must enforce proper
// synchronization.
type slidingHistogram struct {
	windowed *hdrhistogram.WindowedHistogram
	nextT    time.Time
	duration time.Duration
}

// newSlidingHistogram creates a new windowed HDRHistogram with the given
// parameters. Data is kept in the active window for approximately the given
// duration. See the documentation for hdrhistogram.WindowedHistogram for
// details.
func newSlidingHistogram(duration time.Duration, maxVal int64, sigFigs int) *slidingHistogram {
	if duration <= 0 {
		panic("cannot create a sliding histogram with nonpositive duration")
	}
	return &slidingHistogram{
		nextT:    now(),
		duration: duration,
		windowed: hdrhistogram.NewWindowed(histWrapNum, 0, maxVal, sigFigs),
	}
}

func (h *slidingHistogram) tick() {
	h.nextT = h.nextT.Add(h.duration / histWrapNum)
	h.windowed.Rotate()
}

func (h *slidingHistogram) nextTick() time.Time {
	return h.nextT
}

func (h *slidingHistogram) Current() *hdrhistogram.Histogram {
	maybeTick(h)
	return h.windowed.Merge()
}

func (h *slidingHistogram) RecordValue(v int64) error {
	return h.windowed.Current.RecordValue(v)
}
