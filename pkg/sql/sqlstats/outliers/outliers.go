// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package outliers

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// LatencyThreshold configures the execution time beyond which a statement is
// considered an outlier. A LatencyThreshold of 0 (the default) disables the
// outliers subsystem. This setting lives in an "experimental" namespace
// because we expect to supplant threshold-based detection with something
// more statistically interesting, see #79451.
var LatencyThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.stats.outliers.experimental.latency_threshold",
	"amount of time after which an executing statement is considered an outlier. Use 0 to disable.",
	0,
)

// maxCacheSize is the number of detected outliers we will retain in memory.
// We choose a small value for the time being to allow us to iterate without
// worrying about memory usage. See #79450.
const (
	maxCacheSize = 10
)

// Registry is the central object in the outliers subsystem. It observes
// statement execution to determine which statements are outliers and
// exposes the set of currently retained outliers.
type Registry struct {
	detector detector

	// Note that this single mutex places unnecessary constraints on outlier
	// detection and reporting. We will develop a higher-throughput system
	// before enabling the outliers subsystem by default.
	mu struct {
		syncutil.RWMutex
		statements map[clusterunique.ID][]*Outlier_Statement
		outliers   *cache.UnorderedCache
	}
}

// New builds a new Registry.
func New(st *cluster.Settings) *Registry {
	config := cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > maxCacheSize
		},
	}
	r := &Registry{detector: latencyThresholdDetector{st: st}}
	r.mu.statements = make(map[clusterunique.ID][]*Outlier_Statement)
	r.mu.outliers = cache.NewUnorderedCache(config)
	return r
}

// ObserveStatement notifies the registry of a statement execution.
func (r *Registry) ObserveStatement(
	sessionID clusterunique.ID, statementID clusterunique.ID, latencyInSeconds float64,
) {
	if !r.enabled() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.statements[sessionID] = append(r.mu.statements[sessionID], &Outlier_Statement{
		ID:               statementID.GetBytes(),
		LatencyInSeconds: latencyInSeconds,
	})
}

// ObserveTransaction notifies the registry of the end of a transaction.
func (r *Registry) ObserveTransaction(sessionID clusterunique.ID, txnID uuid.UUID) {
	if !r.enabled() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	statements := r.mu.statements[sessionID]
	delete(r.mu.statements, sessionID)

	hasOutlier := false
	for _, s := range statements {
		if r.detector.isOutlier(s) {
			hasOutlier = true
		}
	}

	if hasOutlier {
		for _, s := range statements {
			r.mu.outliers.Add(uint128.FromBytes(s.ID), &Outlier{
				Session:     &Outlier_Session{ID: sessionID.GetBytes()},
				Transaction: &Outlier_Transaction{ID: &txnID},
				Statement:   s,
			})
		}
	}
}

func (r *Registry) enabled() bool {
	return r.detector.enabled()
}

// Reader offers read-only access to the currently retained set of outliers.
type Reader interface {
	IterateOutliers(context.Context, func(context.Context, *Outlier))
}

// IterateOutliers calls visitor with each of the currently retained set of
// outliers.
func (r *Registry) IterateOutliers(ctx context.Context, visitor func(context.Context, *Outlier)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.mu.outliers.Do(func(e *cache.Entry) {
		visitor(ctx, e.Value.(*Outlier))
	})
}
