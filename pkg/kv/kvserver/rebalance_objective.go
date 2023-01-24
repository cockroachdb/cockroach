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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
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
// qps: which is the original default setting and looks at the number of batch
//
//	requests, on a range and store.
//
// cpu: which is added in 23.1 and looks at the cpu usage of a range and store.
type LBRebalancingObjective int64

const (
	// LBRebalancingQueries is a rebalancing mode that balances queries (QPS).
	LBRebalancingQueries LBRebalancingObjective = iota

	// LBRebalancingCPU is a rebalance mode that balances the store CPU usage.
	// The store cpu usage is calculated as the sum of replica's cpu usage on
	// the store.
	LBRebalancingCPU
)

// LoadBasedRebalancingObjective is a cluster setting that defines the load
// balancing objective of the cluster.
var LoadBasedRebalancingObjective = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"kv.allocator.load_based_rebalancing.objective",
	"what objective does the cluster use to rebalance; if set to `qps` "+
		"the cluster will attempt to balance qps among stores, if set to "+
		"`cpu` the cluster will attempt to balance cpu usage among stores",
	"qps",
	map[int64]string{
		int64(LBRebalancingQueries): "qps",
		int64(LBRebalancingCPU):     "cpu",
	},
).WithPublic()

// ToDimension returns the equivalent allocator load dimension of a rebalancing
// objective.
//
// TODO(kvoli): It is currently the case that every LBRebalancingObjective maps
// uniquely to a load.Dimension. However, in the future it is forseeable that
// LBRebalancingObjective could be a value that encompassese many different
// dimension within a single objective e.g. bytes written, cpu usage and
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
// mixed versions exist.
type RebalanceObjectiveProvider interface {
	// Objective returns the current rebalance objective.
	Objective() LBRebalancingObjective
}

// RebalanceObjectiveManager implements the RebalanceObjectiveProvider
// interface and registering a callback at creation time, that will be called
// on a reblanace objective change.
type RebalanceObjectiveManager struct {
	st *cluster.Settings
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
	st *cluster.Settings,
	onChange func(ctx context.Context, obj LBRebalancingObjective),
) *RebalanceObjectiveManager {
	rom := &RebalanceObjectiveManager{st: st}
	rom.mu.obj = ResolveLBRebalancingObjective(ctx, st)
	rom.mu.onChange = onChange

	LoadBasedRebalancingObjective.SetOnChange(&rom.st.SV, func(ctx context.Context) {
		rom.maybeUpdateRebalanceObjective(ctx)
	})
	rom.st.Version.SetOnChange(func(ctx context.Context, _ clusterversion.ClusterVersion) {
		rom.maybeUpdateRebalanceObjective(ctx)
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

	prev := rom.mu.obj
	new := ResolveLBRebalancingObjective(ctx, rom.st)
	// Nothing to do when the objective hasn't changed.
	if prev == new {
		return
	}

	log.Infof(ctx, "Updating the rebalance objective from %s to %s", prev.ToDimension(), new.ToDimension())

	rom.mu.obj = new
	rom.mu.onChange(ctx, rom.mu.obj)
}

// ResolveLBRebalancingObjective returns the load based rebalancing objective
// for the cluster. In cases where a first objective cannot be used, it will
// return a fallback.
func ResolveLBRebalancingObjective(
	ctx context.Context, st *cluster.Settings,
) LBRebalancingObjective {
	set := LoadBasedRebalancingObjective.Get(&st.SV)
	// Queries should always be supported, return early if set.
	if set == int64(LBRebalancingQueries) {
		return LBRebalancingQueries
	}
	// When the cluster version hasn't finalized to 23.1, some unupgraded
	// stores will not be populating additional fields in their StoreCapacity,
	// in such cases we cannot balance another objective since the data may not
	// exist. Fall back to QPS balancing.
	if !st.Version.IsActive(ctx, clusterversion.V23_1AllocatorCPUBalancing) {
		log.Infof(ctx, "version doesn't support cpu objective, reverting to qps balance objective")
		return LBRebalancingQueries
	}
	// When the cpu timekeeping utility is unsupported on this aarch, the cpu
	// usage cannot be gathered. Fall back to QPS balancing.
	if !grunning.Supported() {
		log.Infof(ctx, "cpu timekeeping unavailable on host, reverting to qps balance objective")
		return LBRebalancingQueries
	}
	// The cluster is on a supported version and this local store is on aarch
	// which supported the cpu timekeeping utility, return the cluster setting
	// as is.
	return LBRebalancingObjective(set)
}
