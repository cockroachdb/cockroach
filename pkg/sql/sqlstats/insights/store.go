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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/obs"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	v1 "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/common/v1"
	otel_logs_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

func (s *lockingStore) ExportInsights(
	ctx context.Context, pool *sync.Pool, eventsExporter *obs.EventsExporterInterface,
) error {
	s.mu.Lock()
	var err error
	defer s.mu.Unlock()
	var keysToDelete []uuid.UUID

	s.mu.insights.Do(func(e *cache.Entry) {
		insight := e.Value.(*Insight)

		if insight == nil {
			return
		}
		var fromPool *obspb.StatementInsightsStatistics
		defer pool.Put(fromPool)
		for _, stmt := range insight.Statements {
			fromPool = pool.Get().(*obspb.StatementInsightsStatistics)
			stmt.CopyTo(ctx, insight.Transaction, &insight.Session, fromPool)
			statBytes, e := protoutil.Marshal(fromPool)
			pool.Put(fromPool)
			if e != nil {
				err = e
				return
			}
			(*eventsExporter).SendEvent(ctx, obspb.StatementInsightsStatsEvent, &otel_logs_pb.LogRecord{
				TimeUnixNano: uint64(timeutil.Now().UnixNano()),
				Body:         &v1.AnyValue{Value: &v1.AnyValue_BytesValue{BytesValue: statBytes}},
			})
		}

		keysToDelete = append(keysToDelete, insight.Transaction.ID)
	})

	for _, key := range keysToDelete {
		s.mu.insights.Del(key)
	}

	return err
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
