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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type LockingStore struct {
	// stmtCount keeps track of the number of statement insights
	// that have been observed in the underlying cache.
	stmtCount atomic.Int64

	mu struct {
		syncutil.RWMutex
		insights *cache.UnorderedCache
	}
}

func (s *LockingStore) addInsight(insight *Insight) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stmtCount.Add(int64(len(insight.Statements)))
	s.mu.insights.Add(insight.Transaction.ID, insight)
}

func (s *LockingStore) IterateInsights(
	ctx context.Context, visitor func(context.Context, *Insight),
) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.mu.insights.Do(func(e *cache.Entry) {
		visitor(ctx, e.Value.(*Insight))
	})
}

func newStore(st *cluster.Settings) *LockingStore {
	s := &LockingStore{}
	config := cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return s.stmtCount.Load() > ExecutionInsightsCapacity.Get(&st.SV)
		},
		OnEvicted: func(_, value interface{}) {
			i := value.(*Insight)
			s.stmtCount.Add(-int64(len(i.Statements)))
			releaseInsight(i)
		},
	}

	s.mu.insights = cache.NewUnorderedCache(config)
	return s
}
