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
	causes   *causes

	// Note that this single mutex places unnecessary constraints on outlier
	// detection and reporting. We will develop a higher-throughput system
	// before enabling the insights subsystem by default.
	mu struct {
		syncutil.RWMutex
		statements map[clusterunique.ID][]*Statement
		insights   *cache.OrderedCache
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
			var p Problem
			var c []Cause
			if _, ok := slowStatements[s.ID]; ok {
				switch s.Status {
				case Statement_Completed:
					p = Problem_SlowExecution
					c = r.causes.examine(s)
				case Statement_Failed:
					// Note that we'll be building better failure support for 23.1.
					// For now, we only mark failed statements that were also slow.
					p = Problem_FailedExecution
				}
			}
			r.mu.insights.Add(s.ID, &Insight{
				Session:     &Session{ID: sessionID},
				Transaction: transaction,
				Statement:   s,
				Problem:     p,
				Causes:      c,
			})
		}
	}
}

func (r *lockingRegistry) IterateInsights(
	ctx context.Context, visitor func(context.Context, *Insight),
) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.mu.insights.Do(func(_, v interface{}) bool {
		visitor(ctx, v.(*Insight))
		return false
	})
}

// TODO(todd):
//
//	Once we can handle sufficient throughput to live on the hot
//	execution path in #81021, we can probably get rid of this external
//	concept of "enabled" and let the detectors just decide for themselves
//	internally.
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
		causes:   &causes{st: st},
	}
	r.mu.statements = make(map[clusterunique.ID][]*Statement)
	r.mu.insights = cache.NewOrderedCache(config)
	return r
}
