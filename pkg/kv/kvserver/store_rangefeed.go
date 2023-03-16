// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

type rangeFeedUpdaterConf struct {
	settings *cluster.Settings
	changed  <-chan struct{}
}

func newRangeFeedUpdaterConf(st *cluster.Settings) rangeFeedUpdaterConf {
	confCh := make(chan struct{}, 1)
	confChanged := func(ctx context.Context) {
		select {
		case confCh <- struct{}{}:
		default:
		}
	}
	closedts.SideTransportCloseInterval.SetOnChange(&st.SV, confChanged)
	RangeFeedRefreshInterval.SetOnChange(&st.SV, confChanged)
	RangeFeedSchedulingInterval.SetOnChange(&st.SV, confChanged)
	return rangeFeedUpdaterConf{settings: st, changed: confCh}
}

// get returns a pair of (refresh duration, scheduling duration) which
// determines pacing of the rangefeed updater job.
func (r rangeFeedUpdaterConf) get() (time.Duration, time.Duration) {
	refresh := RangeFeedRefreshInterval.Get(&r.settings.SV)
	if refresh <= 0 {
		refresh = closedts.SideTransportCloseInterval.Get(&r.settings.SV)
	}
	if refresh <= 0 {
		return 0, 0
	}
	sched := RangeFeedSchedulingInterval.Get(&r.settings.SV)
	if sched <= 0 || sched > refresh {
		sched = refresh
	}
	return refresh, sched
}

// wait blocks until it receives a valid rangefeed pacing configuration.
func (r rangeFeedUpdaterConf) wait(ctx context.Context) (time.Duration, time.Duration, error) {
	for {
		if refresh, sched := r.get(); refresh != 0 && sched != 0 {
			return refresh, sched, nil
		}
		select {
		case <-r.changed:
			// Loop back around and check if the config is good now.
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
	}
}

// rangeFeedUpdaterPace returns the number of work items to do (out of the given
// remaining work amount) within the given quant of time. It assumes that work
// can be done at constant speed and uniformly fill the remaining time.
func rangeFeedUpdaterPace(remainingTime, quant time.Duration, remainingWork int) int {
	// If time is running out or there is no more work, do all the remaining work.
	if remainingTime <= quant || remainingWork == 0 {
		return remainingWork
	}
	todo := int(float64(remainingWork) * quant.Seconds() / remainingTime.Seconds())
	if todo > remainingWork { // should never happen, but just in case float64 has quirks
		return remainingWork
	} else if todo == 0 {
		return 1 // always do some work if remainingWork != 0
	}
	return todo
}
