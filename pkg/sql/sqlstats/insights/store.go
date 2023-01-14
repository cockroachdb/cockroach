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
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type lockingStore struct {
	mu struct {
		syncutil.RWMutex
		insights *cache.UnorderedCache
	}
}

func (s *lockingStore) AddInsight(insight *Insight) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.insights.Add(insight.Transaction.ID, insight)
}

func (s *lockingStore) IterateInsights(
	ctx context.Context, visitor func(context.Context, *Insight),
) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.mu.insights.Do(func(e *cache.Entry) {
		visitor(ctx, e.Value.(*Insight))
	})
}

var _ Reader = &lockingStore{}
var _ sink = &lockingStore{}

func newStore(st *cluster.Settings) *lockingStore {
	config := cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return int64(size) > ExecutionInsightsCapacity.Get(&st.SV)
		},
		OnEvicted: func(_, value interface{}) {
			releaseInsight(value.(*Insight))
		},
	}

	s := &lockingStore{}
	s.mu.insights = cache.NewUnorderedCache(config)
	return s
}
