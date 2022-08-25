// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package discovery

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const defaultPollingInterval = time.Second * 10

// TargetsRegistry is responsible for polling the target discovery mechanism for
// an up-to-date list of obs targets. The registry is also responsible for keeping
// track of changes in the list of targets from previous runs and calling
// `updatesCallback()` with lists of targets to add and targets to remove.
type TargetsRegistry struct {
	stop            *stop.Stopper
	targetDiscovery *GossipTargetDiscovery
	targetsByNodeID map[roachpb.NodeID]Target
	updatesCallback func(targetsToAdd []Target, targetsToRemove []Target)
	pollingInterval time.Duration
}

// NewTargetRegistry returns a TargetsRegistry.
func NewTargetRegistry(
	targetDiscovery *GossipTargetDiscovery,
	stop *stop.Stopper,
	updatesCallback func(targetsToAdd []Target, targetsToRemove []Target),
) *TargetsRegistry {
	r := &TargetsRegistry{
		stop:            stop,
		targetDiscovery: targetDiscovery,
	}
	r.pollingInterval = defaultPollingInterval
	r.targetsByNodeID = make(map[roachpb.NodeID]Target)
	r.updatesCallback = updatesCallback
	return r
}

// Start the target discovery as an async process.
func (r *TargetsRegistry) Start(ctx context.Context) {
	_ = r.stop.RunAsyncTask(ctx, "target registry", func(ctx context.Context) {
		ctx, cancel := r.stop.WithCancelOnQuiesce(ctx)
		defer cancel()
		r.run(ctx)
	})
}

func (r *TargetsRegistry) run(ctx context.Context) {
	t := timeutil.NewTimer()
	t.Reset(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			t.Read = true
			newTargets, err := r.getTargetsWithRetry(ctx)
			if err != nil {
				log.Errorf(ctx, "target registry unable to discover targets: %v", err)
				// Unable to get new targets; we will try again later.
				t.Reset(r.pollingInterval)
				continue
			}
			r.updateTargets(newTargets)
			t.Reset(r.pollingInterval)
		}
	}
}

// updateTargets generates a list of targets to add and targets to remove from
// the current and new state received. It is responsible for calling the callback
// function that will let the caller of TargetsRegistry know of any changes in
// known targets.
func (r *TargetsRegistry) updateTargets(newTargets []Target) {
	var targetsToAdd []Target
	var targetsToRemove []Target

	for _, target := range newTargets {
		_, exists := r.targetsByNodeID[target.nodeID]
		switch {
		case !exists:
			// new target: if active, we add to the list of targets to start monitoring
			r.targetsByNodeID[target.nodeID] = target
			if target.active {
				targetsToAdd = append(targetsToAdd, target)
			}
		case !r.targetsByNodeID[target.nodeID].active && target.active:
			// target was previously inactive, but now active
			r.targetsByNodeID[target.nodeID] = target
			targetsToAdd = append(targetsToAdd, target)
		case r.targetsByNodeID[target.nodeID].active && !target.active:
			// target was previously active, but not anymore
			r.targetsByNodeID[target.nodeID] = target
			targetsToRemove = append(targetsToRemove, target)
		}
	}
	if len(targetsToRemove)+len(targetsToAdd) != 0 {
		r.updatesCallback(targetsToAdd, targetsToRemove)
	}
}

// getTargetsWithRetry calls target discovery list function using the last used
// target. In case of errors, it will cycle through the list of all known
// targets, including the initial target passed in on obsservice start-up.
func (r *TargetsRegistry) getTargetsWithRetry(ctx context.Context) ([]Target, error) {
	var targets []Target
	targets, err := r.targetDiscovery.list(ctx)
	if err != nil {
		targets, err = r.tryAllTargets(ctx)
		if err != nil {
			return nil, err
		}
	}
	return targets, err
}

func (r *TargetsRegistry) tryAllTargets(ctx context.Context) ([]Target, error) {
	var targets []Target
	var err error
	knownAddrSet := make(map[string]bool)
	for _, target := range r.targetsByNodeID {
		if target.active {
			knownAddrSet[target.pgUrl] = true
		}
	}
	if _, ok := knownAddrSet[r.targetDiscovery.config.InitPGUrl]; !ok {
		knownAddrSet[r.targetDiscovery.config.InitPGUrl] = true
	}
	for pgUrl := range knownAddrSet {
		if err != nil || ctx.Err() != nil {
			return nil, err
		}
		targetConnCfg, err := pgx.ParseConfig(pgUrl)
		if err != nil {
			continue
		}
		targetConnCfg.Database = "defaultdb"
		targetConn, err := pgx.ConnectConfig(ctx, targetConnCfg)
		if err != nil {
			continue
		}
		err = r.targetDiscovery.db.Close(ctx)
		r.targetDiscovery.db = targetConn
		targets, err = r.targetDiscovery.list(ctx)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, errors.Wrap(err, "target registry failed after trying all known targets")
	}
	return targets, nil
}
