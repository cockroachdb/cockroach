// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// QueryTimespan describes the time range information for a query - the start
// and end bounds of the query, along with the requested duration of individual
// samples to be returned. Methods of this structure are mutating.
type QueryTimespan struct {
	StartNanos          int64
	EndNanos            int64
	NowNanos            int64
	SampleDurationNanos int64
}

// width returns the width of the timespan: the distance between its start and
// and end bounds.
func (qt *QueryTimespan) width() int64 {
	return qt.EndNanos - qt.StartNanos
}

// moveForward modifies the timespan so that it has the same width, but
// both StartNanos and EndNanos is moved forward by the specified number of
// nanoseconds.
func (qt *QueryTimespan) moveForward(forwardNanos int64) {
	qt.StartNanos += forwardNanos
	qt.EndNanos += forwardNanos
}

// expand modifies the timespan so that its width is expanded *on each side*
// by the supplied size; the resulting width will be (2 * size) larger than the
// original width.
func (qt *QueryTimespan) expand(size int64) {
	qt.StartNanos -= size
	qt.EndNanos += size
}

// normalize modifies startNanos and endNanos so that they are exact multiples
// of the sampleDuration. Values are modified by subtraction.
func (qt *QueryTimespan) normalize() {
	qt.StartNanos -= qt.StartNanos % qt.SampleDurationNanos
	qt.EndNanos -= qt.EndNanos % qt.SampleDurationNanos
}

// verifyBounds returns an error if the bounds of this QueryTimespan are
// incorrect; currently, this only occurs if the width is negative.
func (qt *QueryTimespan) verifyBounds() error {
	if qt.StartNanos > qt.EndNanos {
		return fmt.Errorf("startNanos %d was later than endNanos %d", qt.StartNanos, qt.EndNanos)
	}
	return nil
}

// verifyDiskResolution returns an error if this timespan is not suitable for
// querying the supplied disk resolution.
func (qt *QueryTimespan) verifyDiskResolution(diskResolution Resolution) error {
	resolutionSampleDuration := diskResolution.SampleDuration()
	// Verify that sampleDuration is a multiple of
	// diskResolution.SampleDuration().
	if qt.SampleDurationNanos < resolutionSampleDuration {
		return fmt.Errorf(
			"sampleDuration %d was not less that queryResolution.SampleDuration %d",
			qt.SampleDurationNanos,
			resolutionSampleDuration,
		)
	}
	if qt.SampleDurationNanos%resolutionSampleDuration != 0 {
		return fmt.Errorf(
			"sampleDuration %d is not a multiple of queryResolution.SampleDuration %d",
			qt.SampleDurationNanos,
			resolutionSampleDuration,
		)
	}
	return nil
}

// adjustForCurrentTime adjusts the passed query timespan in order to prevent
// certain artifacts which can occur when querying in the very recent past.
func (qt *QueryTimespan) adjustForCurrentTime(diskResolution Resolution) error {
	// Disallow queries for the sample period containing the current system time
	// and any later periods. This prevents returning "incomplete" data for sample
	// periods where new data may yet be recorded, which in turn prevents an odd
	// user experience where graphs of recent metric data have a precipitous "dip"
	// at latest timestamp.
	cutoff := qt.NowNanos - qt.SampleDurationNanos

	// Do not allow queries in the future.
	if qt.StartNanos > cutoff {
		return fmt.Errorf(
			"cannot query time series in the future (start time %s was greater than "+
				"cutoff for current sample period %s); current time: %s; sample duration: %s",
			timeutil.Unix(0, qt.StartNanos),
			timeutil.Unix(0, cutoff),
			timeutil.Unix(0, qt.NowNanos),
			time.Duration(qt.SampleDurationNanos),
		)
	}
	if qt.EndNanos > cutoff {
		qt.EndNanos = cutoff
	}

	return nil
}
