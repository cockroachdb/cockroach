// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type PolicyRefresher struct {
	stopper         *stop.Stopper
	st              *cluster.Settings
	enabled         atomic.Bool
	getLeaseholders func() []Replica
	getLatencyInfos func() map[roachpb.NodeID]time.Duration
}

func NewPolicyRefresher(
	stopper *stop.Stopper,
	st *cluster.Settings,
	getLeaseholders func() []Replica,
	getLatencyInfos func() map[roachpb.NodeID]time.Duration,
) *PolicyRefresher {
	refresher := &PolicyRefresher{
		stopper:         stopper,
		st:              st,
		getLeaseholders: getLeaseholders,
		getLatencyInfos: getLatencyInfos,
	}
	return refresher
}

type Replica interface {
	RefreshLatency(map[roachpb.NodeID]time.Duration)
}

func (pr *PolicyRefresher) IsEnabled() bool {
	return pr.enabled.Load()
}

func (pr *PolicyRefresher) DisableAutoTune() {
	pr.enabled.Store(false)
}

func (pr *PolicyRefresher) RefreshPolicies(leaseholders []Replica) {
	pr.enabled.Store(true)
	latencyInfos := pr.getLatencyInfos()
	for _, lh := range leaseholders {
		lh.RefreshLatency(latencyInfos)
	}
}

func (pr *PolicyRefresher) Run(ctx context.Context) {
	confForLatencyTrackerCh := make(chan struct{}, 1)
	// Note that the config channel doesn't listen to cluster version changes. We
	// hope the timer for side_transport_interval to be short enough if
	// auto-tuning is enabled.
	confChangedForLatencyTracker := func(ctx context.Context) {
		select {
		case confForLatencyTrackerCh <- struct{}{}:
		default:
		}
	}
	closedts.LeadForGlobalReadsAutoTuneInterval.SetOnChange(&pr.st.SV, confChangedForLatencyTracker)

	_ /* err */ = pr.stopper.RunAsyncTask(ctx, "closedts side-transport publisher",
		func(ctx context.Context) {
			var timer timeutil.Timer
			defer timer.Stop()
			var timerForLatencyTracker timeutil.Timer
			defer timerForLatencyTracker.Stop()
			for {
				intervalForLatencyTracker := closedts.LeadForGlobalReadsAutoTuneInterval.Get(&pr.st.SV)
				if intervalForLatencyTracker > 0 && pr.st.Version.IsActive(context.TODO(), clusterversion.V25_2) {
					timerForLatencyTracker.Reset(intervalForLatencyTracker)
				} else {
					// Disable the latency tracker.
					timerForLatencyTracker.Stop()
					pr.DisableAutoTune()
				}
				select {
				case <-timerForLatencyTracker.C:
					timerForLatencyTracker.Read = true
					pr.RefreshPolicies(pr.getLeaseholders())
				case <-confForLatencyTrackerCh:
					continue
				case <-pr.stopper.ShouldQuiesce():
					return
				}
			}
		})
}
