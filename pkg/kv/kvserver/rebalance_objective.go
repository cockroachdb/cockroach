// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// LBRebalancingObjective controls the objective of load based rebalancing.
// This is used to both (1) define the types of load considered when
// determining how balanced the cluster is, and (2) select actions that improve
// balancing the given objective. Currently there are only two possible
// objectives:
//   - qps which is the original default setting and looks at the number of batch
//     requests on a range and store.
//   - cpu which is added in 23.1 and looks at the cpu usage of a range and
//     store.
type LBRebalancingObjective int64

const (
	// LBRebalancingQueries is a rebalancing objective that aims to balances
	// queries (QPS) among stores in the cluster. The QPS per-store is
	// calculated as the sum of every replica's QPS on the store. The QPS value
	// per-replica is calculated as the average number of batch requests per
	// second, the replica received over the last 30 minutes, or replica
	// lifetime, whichever is shorter. A special case for the QPS calculation
	// of a batch request exists for requests that contain AddSST requests,
	// which are weighted by the size of the SST to be added (see #76252). When
	// there are multiple stores per-node, the behavior doesn't change in
	// comparison to single store per-node.
	//
	// When searching for rebalance actions, this objective estimates the
	// impact of an action by using the QPS of the leaseholder replica invovled
	// e.g. the impact of lease transfers on the stores invovled is
	// +leaseholder replica QPS on the store that receives the lease and
	// -leaseholder replica QPS on the store that removes the lease.
	//
	// This rebalancing objective tends to works well when the load of
	// different batch requests in the cluster is uniform. e.g. there are only
	// few types of requests which all exert approx the same load on the
	// system. This rebalancing objective tends to perform poorly when the load
	// of different batch requests in the cluster is non-uniform as balancing
	// QPS does not correlate well with balancing load.
	LBRebalancingQueries LBRebalancingObjective = iota

	// LBRebalancingCPU is a rebalance objective that aims balances the store
	// CPU usage. The store CPU usage is calculated as the sum of replicas' cpu
	// usage on the store. The CPU value per-replica is calculated as the
	// average cpu usage per second, the replica used in processing over the
	// last 30 minutes, or replica lifetime, whichever is shorter. When there
	// are multiple stores per-node, the behavior doesn't change in comparison
	// to single store per-node. That is, despite multiple stores sharing the
	// same underling CPU, the objective attempts to balance CPU usage of each
	// store on a node e.g. In a cluster where there is 1 node and 8 stores on
	// the 1 node, the rebalance objective will rebalance leases and replicas
	// so that the CPU usage is balanced between the 8 stores.
	//
	// When searching for rebalance actions, this objective estimates the
	// impact of an action by either using all of the leaseholder replicas' CPU
	// usage for transfer+rebalance and the foreground request cpu usage for
	// just lease transfers. See allocator/range_usage_info.go.
	//
	// One alternative approach that was considered for the LBRebalancingCPU
	// objective was to use the process CPU usage and balance each stores'
	// process usage. The measured replica cpu usage is used only to determine
	// which replica to rebalance, but not when to rebalance or who to
	// rebalance to. This approach benefits from observing the "true" cpu
	// usage, rather than just the sum of replica's usage. However, unlike the
	// implemented approach, the estimated impact of actions was less reliable
	// and had to be scaled to account for multi-store and missing cpu
	// attribution. The implemented approach composes well in comparison to the
	// process cpu approach. The sum of impact over available actions is equal
	// to the store value being balanced, similar to LBRebalancingQueries.
	LBRebalancingCPU
)

// LoadBasedRebalancingObjectiveMap maps the LoadBasedRebalancingObjective enum
// value to a string.
var LoadBasedRebalancingObjectiveMap = map[LBRebalancingObjective]string{
	LBRebalancingQueries: "qps",
	LBRebalancingCPU:     "cpu",
}

func (lbro LBRebalancingObjective) String() string {
	return LoadBasedRebalancingObjectiveMap[lbro]
}

// LoadBasedRebalancingObjective is a cluster setting that defines the load
// balancing objective of the cluster.
var LoadBasedRebalancingObjective = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"kv.allocator.load_based_rebalancing.objective",
	"what objective does the cluster use to rebalance; if set to `qps` "+
		"the cluster will attempt to balance qps among stores, if set to "+
		"`cpu` the cluster will attempt to balance cpu usage among stores",
	"cpu",
	LoadBasedRebalancingObjectiveMap,
	settings.WithPublic)

// ToDimension returns the equivalent allocator load dimension of a rebalancing
// objective.
//
// TODO(kvoli): It is currently the case that every LBRebalancingObjective maps
// uniquely to a load.Dimension. However, in the future it is forseeable that
// LBRebalancingObjective could be a value that encompassese many different
// dimensions within a single objective e.g. bytes written, cpu usage and
// storage availability. If this occurs, this ToDimension fn will no longer be
// appropriate for multi-dimension objectives.
func (d LBRebalancingObjective) ToDimension() load.Dimension {
	switch d {
	case LBRebalancingQueries:
		return load.Queries
	case LBRebalancingCPU:
		return load.CPU
	default:
		panic("unknown dimension")
	}
}

// RebalanceObjectiveManager provides a method to get the rebalance objective
// of the cluster. It is possible that the cluster setting objective may not be
// the objective returned, when the cluster environment is unsupported or mixed
// versions exist.
type RebalanceObjectiveProvider interface {
	// Objective returns the current rebalance objective.
	Objective() LBRebalancingObjective
}

// gossipStoreDescriptorProvider provides a method to get the store descriptors
// from the storepool, received via gossip. Expose a thin interface for the
// objective manager to use for easier testing.
type gossipStoreDescriptorProvider interface {
	// GetStores returns information on all the stores with descriptor that
	// have been recently seen in gossip.
	GetStores() map[roachpb.StoreID]roachpb.StoreDescriptor
}

// gossipStoreCapacityChangeNotifier provides a method to install a callback
// that will be called whenever the capacity of a store changes. Expose a thin
// interface for the objective manager to use for easier testing.
type gossipStoreCapacityChangeNotifier interface {
	// SetOnCapacityChange installs a callback to be called when the store
	// capacity changes.
	SetOnCapacityChange(fn storepool.CapacityChangeFn)
}

// RebalanceObjectiveManager implements the RebalanceObjectiveProvider
// interface and registers a callback at creation time, that will be called on
// a reblanace objective change.
type RebalanceObjectiveManager struct {
	log.AmbientContext
	st                *cluster.Settings
	storeDescProvider gossipStoreDescriptorProvider

	mu struct {
		syncutil.RWMutex
		obj LBRebalancingObjective
		// onChange callback registered will execute synchronously on the
		// cluster settings thread that triggers an objective check. This is
		// not good for large blocking operations.
		onChange func(ctx context.Context, obj LBRebalancingObjective)
	}
}

func newRebalanceObjectiveManager(
	ctx context.Context,
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	onChange func(ctx context.Context, obj LBRebalancingObjective),
	storeDescProvider gossipStoreDescriptorProvider,
	capacityChangeNotifier gossipStoreCapacityChangeNotifier,
) *RebalanceObjectiveManager {
	rom := &RebalanceObjectiveManager{
		st:                st,
		storeDescProvider: storeDescProvider,
		AmbientContext:    ambientCtx,
	}
	rom.AddLogTag("rebalance-objective", nil)
	ctx = rom.AnnotateCtx(ctx)

	rom.mu.obj = ResolveLBRebalancingObjective(ctx, st, storeDescProvider.GetStores())
	rom.mu.onChange = onChange

	LoadBasedRebalancingObjective.SetOnChange(&rom.st.SV, func(ctx context.Context) {
		rom.maybeUpdateRebalanceObjective(rom.AnnotateCtx(ctx))
	})
	rom.st.Version.SetOnChange(func(ctx context.Context, _ clusterversion.ClusterVersion) {
		rom.maybeUpdateRebalanceObjective(rom.AnnotateCtx(ctx))
	})
	// Rather than caching each capacity locally, use the callback as a trigger
	// to recalculate the objective. This is less expensive than recacluating
	// the objective on every call to Objective, which would need to be done
	// otherwise, just in case a new capacity has come in. This approach does
	// have the downside of using the gossip callback goroutine to trigger the
	// onChange callback, which iterates through every replica on the store. It
	// is unlikely though that the conditions are satisfied (some node begins
	// not supporting grunning or begin supporting grunning) to trigger the
	// onChange callback here.
	capacityChangeNotifier.SetOnCapacityChange(
		func(storeID roachpb.StoreID, old, cur roachpb.StoreCapacity) {
			if (old.CPUPerSecond < 0) != (cur.CPUPerSecond < 0) {
				// NB: On capacity changes we don't have access to a context. Create a
				// background context on callback.
				cbCtx, span := rom.AnnotateCtxWithSpan(context.Background(), "capacity-change")
				defer span.Finish()
				rom.maybeUpdateRebalanceObjective(cbCtx)
			}
		})

	return rom
}

// Objective returns the current rebalance objective.
func (rom *RebalanceObjectiveManager) Objective() LBRebalancingObjective {
	rom.mu.RLock()
	defer rom.mu.RUnlock()

	return rom.mu.obj
}

func (rom *RebalanceObjectiveManager) maybeUpdateRebalanceObjective(ctx context.Context) {
	rom.mu.Lock()
	defer rom.mu.Unlock()

	ctx = rom.AnnotateCtx(ctx)
	prev := rom.mu.obj
	next := ResolveLBRebalancingObjective(ctx, rom.st, rom.storeDescProvider.GetStores())
	// Nothing to do when the objective hasn't changed.
	if prev == next {
		return
	}

	log.Infof(ctx, "Updating the rebalance objective from %s to %s",
		prev, next)

	rom.mu.obj = next
	rom.mu.onChange(ctx, rom.mu.obj)
}

// ResolveLBRebalancingObjective returns the load based rebalancing objective
// for the cluster. In cases where a first objective cannot be used, it will
// return a fallback.
func ResolveLBRebalancingObjective(
	ctx context.Context, st *cluster.Settings, descs map[roachpb.StoreID]roachpb.StoreDescriptor,
) LBRebalancingObjective {
	set := LoadBasedRebalancingObjective.Get(&st.SV)
	// Queries should always be supported, return early if set.
	if set == LBRebalancingQueries {
		return LBRebalancingQueries
	}
	// When the cpu timekeeping utility is unsupported on this aarch, the cpu
	// usage cannot be gathered. Fall back to QPS balancing.
	if !grunning.Supported() {
		log.Infof(ctx, "cpu timekeeping unavailable on host, reverting to qps balance objective")
		return LBRebalancingQueries
	}

	// It is possible that the cputime utility isn't supported on a remote
	// node's architecture, yet is supported locally on this node. If that is
	// the case, the store's on the node will publish the cpu per second as -1
	// for their capacity to gossip. The -1 is  special cased here and
	// disallows any other store using the cpu balancing objective.
	for _, desc := range descs {
		if desc.Capacity.CPUPerSecond == -1 {
			log.Warningf(ctx,
				"cpu timekeeping unavailable on node %d but available locally, reverting to qps balance objective",
				desc.Node.NodeID)
			return LBRebalancingQueries
		}
	}

	// The cluster is on a supported version and this local store is on aarch
	// which supported the cpu timekeeping utility, return the cluster setting
	// as is.
	return set
}
