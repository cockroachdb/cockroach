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
	"github.com/jackc/pgx/v4/pgxpool"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const PollingDuration = 10 * time.Second

type TargetsRegistry struct {
	stop            *stop.Stopper
	targetDiscovery targetDiscovery
	mu              struct {
		syncutil.Mutex
		targetsByID map[roachpb.NodeID]Target
	}
	updatesCallback func(targetsToAdd []Target, targetsToRemove []Target)
}

// NewTargetRegistry creates a TargetsRegistry
func NewTargetRegistry(
	targetDiscovery targetDiscovery,
	stop *stop.Stopper,
	updatesCallback func(targetsToAdd []Target, targetsToRemove []Target)) *TargetsRegistry {
	r := &TargetsRegistry{
		stop:            stop,
		targetDiscovery: targetDiscovery,
	}
	r.mu.targetsByID = make(map[roachpb.NodeID]Target)
	r.updatesCallback = updatesCallback
	return r
}

// Start the target discovery as an async process
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
				// unable to get new targets, try again later
				t.Reset(PollingDuration)
				continue
			}
			var targetsToAdd []Target
			var targetsToRemove []Target

			r.mu.Lock()
			for _, target := range newTargets {
				_, exists := r.mu.targetsByID[target.nodeID]
				switch {
				case !exists:
					// new target: if active, add to list
					r.mu.targetsByID[target.nodeID] = target
					if target.active {
						targetsToAdd = append(targetsToAdd, target)
					}
				case !r.mu.targetsByID[target.nodeID].active && target.active:
					// target was previously inactive, but now active
					targetsToAdd = append(targetsToAdd, target)
				case r.mu.targetsByID[target.nodeID].active && !target.active:
					// target was previously active, but not anymore
					targetsToRemove = append(targetsToRemove, target)
				}
			}
			r.mu.Unlock()
			if len(targetsToRemove)+len(targetsToAdd) != 0 {
				r.updatesCallback(targetsToAdd, targetsToRemove)
			}
			t.Reset(PollingDuration)
		}
	}
}

func (r *TargetsRegistry) getTargetsWithRetry(ctx context.Context) ([]Target, error) {
	var targets []Target
	targets, err := r.targetDiscovery.List(ctx)
	if err != nil {
		var knownTargets []Target
		r.mu.Lock()
		for _, target := range r.mu.targetsByID {
			knownTargets = append(knownTargets, target)
		}
		r.mu.Unlock()
		for _, target := range knownTargets {
			if ctx.Err() != nil {
				return nil, err
			}
			// retry with all known targets
			if crdbTargetDiscovery, ok := r.targetDiscovery.(*CRDBTargetDiscovery); ok {
				// connect to target cluster
				targetConnCfg, err := pgxpool.ParseConfig(target.PGURLString())
				if err != nil {
					continue
				}
				targetPool, err := pgxpool.ConnectConfig(ctx, targetConnCfg)
				if err != nil {
					continue
				}
				crdbTargetDiscovery.db = targetPool
				targets, err = r.targetDiscovery.List(ctx)
				if err == nil {
					break
				}
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return targets, nil
}
