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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// PolicyRefresher manages closed timestamp policies for multi-region ranges. It
// periodically refreshes latency based range closed timestamp policies which
// will used to determine how far into the future timestamps should be closed to
// serve present follower reads.
type PolicyRefresher struct {
	stopper *stop.Stopper
	st      *cluster.Settings

	// enabled indicates if the policy refresher is enabled.
	enabled atomic.Bool

	// getLeaseholders returns the leaseholders for which the closed timestamp
	// policy should be refreshed.
	getLeaseholders func() []Replica

	// getLatencyInfos returns the latency information between the current node
	// and other nodes in the cluster. It is up to the replicas to decide what to
	// do if latency information needed is missing.
	getLatencyInfos func() map[roachpb.NodeID]time.Duration
}

func NewPolicyRefresher(
	stopper *stop.Stopper,
	st *cluster.Settings,
	getLeaseholders func() []Replica,
	getLatencyInfos func() map[roachpb.NodeID]time.Duration,
) *PolicyRefresher {
	if getLeaseholders == nil || getLatencyInfos == nil {
		log.Fatalf(context.Background(), "getLeaseholders and getLatencyInfos must be non-nil")
		return nil
	}
	refresher := &PolicyRefresher{
		stopper:         stopper,
		st:              st,
		getLeaseholders: getLeaseholders,
		getLatencyInfos: getLatencyInfos,
	}
	return refresher
}

// Replica is an interface that allows the PolicyRefresher to refresh the
// closed timestamp policy on a replica. Implemented by kvserver.Replica.
type Replica interface {
	RefreshLatency(map[roachpb.NodeID]time.Duration)
}

// IsEnabled returns true if the policy refresher is enabled. It is enabled when
// LeadForGlobalReadsAutoTuneInterval is positive and the cluster version is >=
// v25.2. This is used by replicas to determine if they should use the cached
// latency based closed timestamp policies or not.
func (pr *PolicyRefresher) IsEnabled() bool {
	return pr.enabled.Load()
}

// disableAutoTune disables the policy refresher.
func (pr *PolicyRefresher) disableAutoTune() {
	pr.enabled.Store(false)
}

// RefreshPolicies refreshes the closed timestamp policy for the given
// leaseholders.
func (pr *PolicyRefresher) RefreshPolicies(leaseholders []Replica) {
	if !pr.st.Version.IsActive(context.TODO(), clusterversion.V25_2) {
		return
	}
	pr.enabled.Store(true)
	latencyInfos := pr.getLatencyInfos()
	for _, lh := range leaseholders {
		lh.RefreshLatency(latencyInfos)
	}
}

// Run runs the policy refresher. It will refresh the closed timestamp policy
// for the given leaseholders (pr.getLeaseholders) at the interval specified by
// LeadForGlobalReadsAutoTuneInterval. The caller is responsible for canceling the
// given context to tear down the policy refresher.
func (pr *PolicyRefresher) Run(ctx context.Context) {
	confForLatencyTrackerCh := make(chan struct{}, 1)
	// Note that the config channel doesn't subscribe to cluster version changes. We
	// hope the timer for
	// kv.closed_timestamp.lead_for_global_reads_auto_tune_interval to be short
	// enough to pick that up relatively soon.
	confChangedForLatencyTracker := func(ctx context.Context) {
		select {
		case confForLatencyTrackerCh <- struct{}{}:
		default:
		}
	}
	closedts.LeadForGlobalReadsAutoTuneInterval.SetOnChange(&pr.st.SV, confChangedForLatencyTracker)

	_ /* err */ = pr.stopper.RunAsyncTask(ctx, "closed timestamp policy refresher",
		func(ctx context.Context) {
			var timerForLatencyTracker timeutil.Timer
			defer timerForLatencyTracker.Stop()
			for {
				intervalForLatencyTracker := closedts.LeadForGlobalReadsAutoTuneInterval.Get(&pr.st.SV)
				if intervalForLatencyTracker > 0 {
					timerForLatencyTracker.Reset(intervalForLatencyTracker)
				} else {
					// Disable the latency tracker.
					timerForLatencyTracker.Stop()
					pr.disableAutoTune()
				}
				select {
				case <-timerForLatencyTracker.C:
					timerForLatencyTracker.Read = true
					pr.RefreshPolicies(pr.getLeaseholders())
				case <-confForLatencyTrackerCh:
					continue
				case <-pr.stopper.ShouldQuiesce():
					return
				case <-ctx.Done():
					return
				}
			}
		})
}
