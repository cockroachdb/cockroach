// Copyright 2023The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type RebalanceObjectiveProvider interface {
	Objective() LBRebalancingDimension
}

type RebalanceObjectiveManager struct {
	st *cluster.Settings
	mu struct {
		syncutil.RWMutex
		obj      LBRebalancingDimension
		onChange func(ctx context.Context, obj LBRebalancingDimension)
	}
}

func newRebalanceObjectiveManager(
	ctx context.Context,
	st *cluster.Settings,
	onChange func(ctx context.Context, obj LBRebalancingDimension),
) *RebalanceObjectiveManager {
	rom := &RebalanceObjectiveManager{st: st}
	rom.mu.obj = LoadBasedRebalancingObjective(ctx, st)
	rom.mu.onChange = onChange

	rom.registerTriggers()

	return rom
}

func (rom *RebalanceObjectiveManager) Objective() LBRebalancingDimension {
	rom.mu.RLock()
	defer rom.mu.RUnlock()

	return rom.mu.obj
}

func (rom *RebalanceObjectiveManager) registerTriggers() {
	LoadBasedRebalancingDimension.SetOnChange(&rom.st.SV, func(ctx context.Context) {
		rom.maybeUpdateRebalanceObjective(ctx)
	})
	rom.st.Version.SetOnChange(func(ctx context.Context, _ clusterversion.ClusterVersion) {
		rom.maybeUpdateRebalanceObjective(ctx)
	})
}

func (rom *RebalanceObjectiveManager) maybeUpdateRebalanceObjective(ctx context.Context) {
	rom.mu.Lock()
	defer rom.mu.Unlock()

	prev := rom.mu.obj
	new := LoadBasedRebalancingObjective(ctx, rom.st)
	// Nothing to do when the objective hasn't changed.
	if prev == new {
		return
	}

	log.Infof(ctx, "Updating the rebalance objective from %s to %s", prev.ToDimension(), new.ToDimension())

	rom.mu.obj = new
	rom.mu.onChange(ctx, new)
}

// LoadBasedRebalancingObjective returns the load based rebalancing objective
// for the cluster. In cases where a first objective cannot be used, it will
// return a fallback.
func LoadBasedRebalancingObjective(
	ctx context.Context, st *cluster.Settings,
) LBRebalancingDimension {
	set := LoadBasedRebalancingDimension.Get(&st.SV)
	// Queries should always be supported, return early if set.
	if set == int64(LBRebalancingQueries) {
		return LBRebalancingQueries
	}
	// When the cluster version hasn't finalized to 23.1, some unupgraded
	// stores will not be populating additional fields in their StoreCapacity,
	// in such cases we cannot balance another objective since the data may not
	// exist. Fall back to QPS balancing.
	if !st.Version.IsActive(ctx, clusterversion.V23_1AllocatorCPUBalancing) {
		log.Infof(ctx, "version doesn't support cpu, reverting to qps balance objective")
		return LBRebalancingQueries
	}
	// When the cpu timekeeping utility is unsupported on this aarch, the cpu
	// usage cannot be gathered. Fall back to QPS balancing.
	if !grunning.Supported() {
		log.Infof(ctx, "cpu timekeeping unavailable on host, cannot use cpu, reverting to qps balance objective")
		return LBRebalancingQueries
	}
	// The cluster is on a supported version and this local store is on aarch
	// which supported the cpu timekeeping utility, return the cluster setting
	// as is.
	return LBRebalancingDimension(set)
}
