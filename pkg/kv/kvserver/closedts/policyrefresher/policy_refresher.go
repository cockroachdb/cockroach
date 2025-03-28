// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package policyrefresher

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// PolicyRefresher periodically refreshes the closed timestamp policies for
// leaseholder replicas based on network latencies between leaseholders and
// their furthest follower.
//
// For ranges configured to serve global reads, this determines how far into the
// future timestamps should be closed based on the latency to the farthest
// follower (if LeadForGlobalReadsAutoTuneEnabled is enabled).
type PolicyRefresher struct {
	stopper  *stop.Stopper
	settings *cluster.Settings

	// getLeaseholderReplicas returns the set of replicas that are currently
	// leaseholders of the node.
	getLeaseholderReplicas func() []Replica

	// refreshNotificationCh is used to signal when replicas need their policies
	// refreshed outside the normal refresh interval. They are added on demand
	// when there is a leaseholder change or when there is a span config change.
	refreshNotificationCh chan struct{}

	// getNodeLatencies returns a map of node IDs to their measured latencies
	// from the current node. Replicas use this information to determine
	// appropriate closed timestamp policies.
	getNodeLatencies func() map[roachpb.NodeID]time.Duration

	// latencyCache caches the latency information from getNodeLatencies. This
	// can be expensive, so we only do this periodically based on the configured
	// refresh interval.
	mu struct {
		syncutil.RWMutex
		latencyCache map[roachpb.NodeID]time.Duration
	}

	// rMu protects access to the list of replicas needing policy refresh.
	rMu struct {
		syncutil.Mutex
		pendingReplicas []Replica
	}
}

func NewPolicyRefresher(
	stopper *stop.Stopper,
	settings *cluster.Settings,
	getLeaseholderReplicas func() []Replica,
	getNodeLatencies func() map[roachpb.NodeID]time.Duration,
) *PolicyRefresher {
	if getLeaseholderReplicas == nil || getNodeLatencies == nil {
		log.Fatalf(context.Background(), "getLeaseholderReplicas and getNodeLatencies must be non-nil")
		return nil
	}
	refresher := &PolicyRefresher{
		stopper:                stopper,
		settings:               settings,
		getLeaseholderReplicas: getLeaseholderReplicas,
		getNodeLatencies:       getNodeLatencies,
		refreshNotificationCh:  make(chan struct{}, 1),
	}
	return refresher
}

// Replica is implemented by kvserver.Replica.
type Replica interface {
	// RefreshPolicy is called to update closed timestamp policy periodically
	// with observed latency information by PolichRefresher. Note that the
	// given map here can be nil.
	RefreshPolicy(map[roachpb.NodeID]time.Duration)
}

// detachReplicas atomically retrieves and clears the list of replicas needing
// policy refresh.
func (pr *PolicyRefresher) detachReplicas() []Replica {
	pr.rMu.Lock()
	defer pr.rMu.Unlock()
	toRefresh := pr.rMu.pendingReplicas
	pr.rMu.pendingReplicas = nil
	return toRefresh
}

// EnqueueReplicaForRefresh adds a replica to the list of those needing policy
// refresh and signals the refresh goroutine.
func (pr *PolicyRefresher) EnqueueReplicaForRefresh(replica Replica) {
	if pr == nil {
		return
	}
	pr.rMu.Lock()
	defer pr.rMu.Unlock()
	pr.rMu.pendingReplicas = append(pr.rMu.pendingReplicas, replica)
	// Note that refreshNotificationCh is non-blocking.
	select {
	case pr.refreshNotificationCh <- struct{}{}:
	default:
	}
}

// updateLatencyCache refreshes the cached latency information by fetching fresh
// measurements from the actual RPC context. This can be expensive, so we only
// do this periodically based on the configured refresh interval.
func (pr *PolicyRefresher) updateLatencyCache() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.mu.latencyCache = pr.getNodeLatencies()
}

// getCurrentLatencies returns the current latency information if auto-tuning is
// enabled and the cluster has been fully upgraded to v25.2, or nil otherwise.
func (pr *PolicyRefresher) getCurrentLatencies() map[roachpb.NodeID]time.Duration {
	if !closedts.LeadForGlobalReadsAutoTuneEnabled.Get(&pr.settings.SV) || !pr.settings.Version.IsActive(context.TODO(), clusterversion.V25_2) {
		return nil
	}
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	return pr.mu.latencyCache
}

// refreshPolicies updates the closed timestamp policy for the given
// leaseholders based on current latency information. This can be called from
// the store or from pr.Run.
func (pr *PolicyRefresher) refreshPolicies(leaseholders ...Replica) {
	latencies := pr.getCurrentLatencies()
	for _, leaseholder := range leaseholders {
		leaseholder.RefreshPolicy(latencies)
	}
}

// Run spwans a goroutine that periodically refreshes the closed timestamp
// policies for the leaseholder replicas based on the current latency
// information. It also handles on-demand refresh requests from
// EnqueueReplicaForRefresh. The loop running in the goroutine will block until
// the context is done or the stopper is quiesced.
func (pr *PolicyRefresher) Run(ctx context.Context) {
	configUpdateCh := make(chan struct{}, 1)
	// Note that the config channel doesn't subscribe to cluster version
	// changes. We rely on the relatively short
	// RangeClosedTimestampPolicyRefreshInterval to ensure timely updates when
	// cluster version changes occur.
	onConfigChange := func(ctx context.Context) {
		select {
		case configUpdateCh <- struct{}{}:
		default:
		}
	}
	closedts.RangeClosedTimestampPolicyRefreshInterval.SetOnChange(&pr.settings.SV, onConfigChange)

	_ /* err */ = pr.stopper.RunAsyncTask(ctx, "closed timestamp policy refresher",
		func(ctx context.Context) {
			var refreshTimer timeutil.Timer
			defer refreshTimer.Stop()
			for {
				refreshInterval := closedts.RangeClosedTimestampPolicyRefreshInterval.Get(&pr.settings.SV)
				if refreshInterval > 0 {
					refreshTimer.Reset(refreshInterval)
				} else {
					refreshTimer.Stop()
				}
				select {
				case <-pr.refreshNotificationCh:
					pr.refreshPolicies(pr.detachReplicas()...)
				case <-refreshTimer.C:
					refreshTimer.Read = true
					pr.updateLatencyCache()
					pr.refreshPolicies(pr.getLeaseholderReplicas()...)
				case <-configUpdateCh:
					continue
				case <-pr.stopper.ShouldQuiesce():
					return
				case <-ctx.Done():
					return
				}
			}
		})
}
