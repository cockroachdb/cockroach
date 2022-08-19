// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// This registry is the central object in the insights subsystem. It observes
// statement execution to determine which statements are outliers and
// exposes the set of currently retained insights.
type lockingRegistry struct {
	detector detector
	problems *problems

	// Note that this single mutex places unnecessary constraints on outlier
	// detection and reporting. We will develop a higher-throughput system
	// before enabling the insights subsystem by default.
	mu struct {
		syncutil.RWMutex
		statements map[clusterunique.ID][]*Statement
		insights   *cache.UnorderedCache
	}
}

var _ Writer = &lockingRegistry{}
var _ Reader = &lockingRegistry{}

func (r *lockingRegistry) ObserveStatement(sessionID clusterunique.ID, statement *Statement) {
	if !r.enabled() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.statements[sessionID] = append(r.mu.statements[sessionID], statement)
}

func (r *lockingRegistry) ObserveTransaction(sessionID clusterunique.ID, transaction *Transaction) {
	if !r.enabled() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	statements := r.mu.statements[sessionID]
	delete(r.mu.statements, sessionID)

	slowStatements := make(map[clusterunique.ID]struct{})
	for _, s := range statements {
		if r.detector.isSlow(s) {
			slowStatements[s.ID] = struct{}{}
		}
	}

	if len(slowStatements) > 0 {
		for _, s := range statements {
			var p []Problem
			if _, ok := slowStatements[s.ID]; ok {
				p = r.problems.examine(s)
			}
			r.mu.insights.Add(s.ID, &Insight{
				Session:     &Session{ID: sessionID},
				Transaction: transaction,
				Statement:   s,
				Problems:    p,
			})
		}
	}
}

func (r *lockingRegistry) IterateInsights(
	ctx context.Context, visitor func(context.Context, *Insight),
) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.mu.insights.Do(func(e *cache.Entry) {
		visitor(ctx, e.Value.(*Insight))
	})
}

// TODO(todd):
//   Once we can handle sufficient throughput to live on the hot
//   execution path in #81021, we can probably get rid of this external
//   concept of "enabled" and let the detectors just decide for themselves
//   internally.
func (r *lockingRegistry) enabled() bool {
	return r.detector.enabled()
}

func newRegistry(st *cluster.Settings, detector detector) *lockingRegistry {
	config := cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return int64(size) > ExecutionInsightsCapacity.Get(&st.SV)
		},
	}
	r := &lockingRegistry{
		detector: detector,
		problems: &problems{st: st},
	}
	r.mu.statements = make(map[clusterunique.ID][]*Statement)
	r.mu.insights = cache.NewUnorderedCache(config)
	return r
}
