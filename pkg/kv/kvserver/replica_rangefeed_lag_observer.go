// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/redact"
)

// laggingRangeFeedCTNudgeMultiple is the multiple of the closed timestamp target
// duration that a rangefeed's closed timestamp can lag behind the current time
// before the rangefeed is nudged to catch up.
const laggingRangeFeedCTNudgeMultiple = 5

// RangeFeedLaggingCTCancelMultiple is the multiple of the closed timestamp
// target duration that a rangefeed's closed timestamp can lag behind the
// current time before the rangefeed is cancelled, if the duration threshold is
// also met, see RangeFeedLaggingCTCancelDuration. When set to 0, cancelling is
// disabled.
var RangeFeedLaggingCTCancelMultiple = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.rangefeed.lagging_closed_timestamp_cancel_multiple",
	"if a range's closed timestamp is more than this multiple of the "+
		"`kv.closed_timestamp.target_duration` behind the current time,"+
		"for at least `kv.rangefeed.lagging_closed_timestamp_cancel_min_lagging_duration`"+
		", cancel the rangefeed, when set to 0, canceling is disabled",
	20, /* 20x closed ts target, currently default 60s */
	// NB: We don't want users setting a value incongruent with the closed
	// timestamp target duration, as that would lead to thrashing of rangefeeds.
	// Also, the nudge multiple is a constant above, so we don't want users
	// setting a lower value than that, as nudging is a prerequisite for
	// cancelling.
	settings.IntInRangeOrZeroDisable(laggingRangeFeedCTNudgeMultiple, 10_000),
)

// RangeFeedLaggingCTCancelDuration is the duration threshold for lagging
// rangefeeds to be cancelled when the closed timestamp is lagging behind the
// current time by more than:
//
// `kv.rangefeed.lagging_closed_timestamp_cancel_multiple` *
// `kv.closed_timestamp.target_duration`
//
// e.g., if the closed timestamp target duration is 3s (current default) and
// the multiple is 2, then the lagging rangefeed will be canceled if the closed
// timestamp is more than 6s behind the current time, for at least
// laggingRangeFeedCTCancelDurationThreshold:
//
//		closed_ts                  = -7s (relative to now)
//		target_closed_ts           = -3s
//		multiple                   = 2.0
//	  lagging_duration_threshold = 60s
//
// In the above example, the rangefeed will be canceled if this state is
// sustained for at least 60s. Visually (and abstractly) it looks like this:
//
//	lag=0          ─────────────────────────────────────────────────────
//
//	observed lag   ─────────┐
//	                        │
//	                        │
//	                        │     ┌───────┐
//	lag threshold  ─────────┼─────┼───────┼──────────────────────────────
//	                        │     │       └───┐
//	                        │     │           └─────┐
//	                        └─────┘                 └──────┐
//	                                                       └────────────
//	                                      ◄────────────────────────────►
//	                                         exceeds duration threshold
//
// Where time is moving from left to right, and the y-axis represents the
// closed timestamp lag relative to the current time.
var RangeFeedLaggingCTCancelDuration = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.rangefeed.lagging_closed_timestamp_cancel_min_lagging_duration",
	"if a range's closed timestamp is more than "+
		"`kv.rangefeed.lagging_closed_timestamp_cancel_multiple` of the "+
		"`kv.closed_timestamp.target_duration` behind the current time,"+
		"for at least this duration, cancel the rangefeed",
	time.Minute,
)

type rangeFeedCTLagObserver struct {
	exceedsCancelLagStartTime time.Time
}

func newRangeFeedCTLagObserver() *rangeFeedCTLagObserver {
	return &rangeFeedCTLagObserver{}
}

func (r *rangeFeedCTLagObserver) observeClosedTimestampUpdate(
	ctx context.Context, closedTS, now time.Time, sv *settings.Values,
) rangeFeedCTLagSignal {
	lag := now.Sub(closedTS)
	targetLag := closedts.TargetDuration.Get(sv)
	nudgeLagThreshold := targetLag * laggingRangeFeedCTNudgeMultiple
	cancelLagMinDuration := RangeFeedLaggingCTCancelDuration.Get(sv)
	// When the cancel threshold is set to 0, we disable cancelling.
	cancelLagThreshold := time.Duration(math.MaxInt64)
	if cancelLagMult := RangeFeedLaggingCTCancelMultiple.Get(sv); cancelLagMult > 0 {
		cancelLagThreshold = targetLag * time.Duration(cancelLagMult)
	}

	// Now, calculate the signal
	exceedsCancelLagThreshold := false
	exceedsNudgeLagThreshold := lag > nudgeLagThreshold
	if lag <= cancelLagThreshold {
		// The closed timestamp is no longer lagging behind the current time by
		// more than the cancel threshold, so reset the start time, as we only want
		// to signal on sustained lag above the threshold.
		r.exceedsCancelLagStartTime = time.Time{}
	} else if r.exceedsCancelLagStartTime.IsZero() {
		r.exceedsCancelLagStartTime = now
	} else {
		exceedsCancelLagThreshold = now.Sub(r.exceedsCancelLagStartTime) > cancelLagMinDuration
	}

	return rangeFeedCTLagSignal{
		lag:                       lag,
		targetLag:                 targetLag,
		exceedsNudgeLagThreshold:  exceedsNudgeLagThreshold,
		exceedsCancelLagThreshold: exceedsCancelLagThreshold,
	}
}

type rangeFeedCTLagSignal struct {
	lag                       time.Duration
	targetLag                 time.Duration
	exceedsNudgeLagThreshold  bool
	exceedsCancelLagThreshold bool
}

func (rfls rangeFeedCTLagSignal) String() string {
	return redact.StringWithoutMarkers(rfls)
}

var _ redact.SafeFormatter = rangeFeedCTLagSignal{}

// SafeFormat implements the redact.SafeFormatter interface.
func (rfls rangeFeedCTLagSignal) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf(
		"behind=%v target=%v nudge=%t cancel=%t",
		rfls.lag,
		rfls.targetLag,
		rfls.exceedsNudgeLagThreshold,
		rfls.exceedsCancelLagThreshold,
	)
}
