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

type rangeFeedUpdaterSched struct {
	start    time.Time
	now      time.Time
	deadline time.Time
	step     time.Duration
	items    int // the remaining number of work items
}

func newRangeFeedUpdaterSched(
	now time.Time, dur, step time.Duration, items int,
) rangeFeedUpdaterSched {
	return rangeFeedUpdaterSched{
		start: now, now: now, deadline: now.Add(dur), step: step, items: items,
	}
}

func (r *rangeFeedUpdaterSched) next() (todo int, target time.Time) {
	if r.items <= 0 {
		return 0, r.deadline
	}
	timeLeft := r.deadline.Sub(r.now)
	if timeLeft <= r.step { // including timeLeft <= 0, i.e. the deadline passed
		r.items, todo = 0, r.items
		return todo, r.deadline
	}
	// Assume that from now on we can do the work at constant speed.
	canDo := float64(r.step) / float64(timeLeft) // in [0, 1)
	if todo = int(float64(r.items) * canDo); todo <= 0 {
		todo = 1
	}
	r.items -= todo
	if r.items == 0 { // stretch the last chunk of work until the deadline
		return todo, r.deadline
	}
	return todo, r.now.Add(r.step)
}

func (r *rangeFeedUpdaterSched) tick(now time.Time) {
	r.now = now
}
