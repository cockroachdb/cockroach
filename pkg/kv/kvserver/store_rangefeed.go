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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// rangeFeedUpdaterConf provides configuration for the rangefeed updater job,
// and allows watching for when it is updated.
type rangeFeedUpdaterConf struct {
	settings *cluster.Settings
	changed  <-chan struct{}
}

// newRangeFeedUpdaterConf creates the config reading from and watching the
// given cluster settings.
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
	RangeFeedSmearInterval.SetOnChange(&st.SV, confChanged)
	return rangeFeedUpdaterConf{settings: st, changed: confCh}
}

// get returns a pair of (refresh interval, smear interval) which determines
// pacing of the rangefeed closed timestamp updater job.
func (r rangeFeedUpdaterConf) get() (time.Duration, time.Duration) {
	refresh := RangeFeedRefreshInterval.Get(&r.settings.SV)
	if refresh <= 0 {
		refresh = closedts.SideTransportCloseInterval.Get(&r.settings.SV)
	}
	if refresh <= 0 {
		return 0, 0
	}
	smear := RangeFeedSmearInterval.Get(&r.settings.SV)
	if smear <= 0 || smear > refresh {
		smear = refresh
	}
	return refresh, smear
}

// wait blocks until it receives a valid rangefeed closed timestamp pacing
// configuration, and returns it.
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

// rangeFeedUpdaterPace returns the number of work items to do (out of workLeft)
// within a quantum of time, and a suggested deadline for completing this work.
// It assumes that work can be done at constant speed and uniformly fill the
// remaining time between now and the deadline.
//
// See TestRangeFeedUpdaterPace for an example of how this function can/should
// be used for scheduling work.
func rangeFeedUpdaterPace(
	now, deadline time.Time, quantum time.Duration, workLeft int,
) (todo int, by time.Time) {
	timeLeft := deadline.Sub(now)
	if workLeft <= 0 || timeLeft <= 0 { // ran out of work or time
		return workLeft, now
	} else if timeLeft <= quantum { // time is running out
		return workLeft, deadline
	}
	// Otherwise, we have workLeft >= 1, and at least a full quantum of time.
	// Assume we can complete work at uniform speed.
	todo = int(float64(workLeft) * quantum.Seconds() / timeLeft.Seconds())
	by = now.Add(quantum)
	if todo > workLeft { // should never happen, but just in case float64 has quirks
		return workLeft, by
	} else if todo == 0 {
		return 1, by // always do some work
	}
	return todo, by
}

type rangeFeedScheduler struct {
	r *rangefeed.CallbackScheduler
	s *rangefeed.Scheduler[int, int64]
}

func newRangeFeedScheduler(workerCount int) *rangeFeedScheduler {
	sc := rangefeed.NewScheduler()
	s := &rangeFeedScheduler{
		r: rangefeed.NewCallbackScheduler(fmt.Sprintf("s%d", 1), sc, workerCount),
		s: sc,
	}
	return s
}

func (r *rangeFeedScheduler) Start(stopper *stop.Stopper) {
	r.r.Start(stopper)
}

func (r *rangeFeedScheduler) RegisterReplica(id roachpb.RangeID, f rangefeed.Callback) {
	_ = r.r.Register(int64(id), f)
}

func (r *rangeFeedScheduler) UnregisterReplica(id roachpb.RangeID) {
	r.r.Unregister(int64(id))
}

// Close ungracefully keeping on garbage in the queues.
func (r *rangeFeedScheduler) Close()  {
	r.r.Close()
}
