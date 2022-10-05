// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package structlogging

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// hotRangesDetailsLoggingScheduler is responsible for logging index usage stats
// on a scheduled interval.
type hotRangesDetailsLoggingScheduler struct {
	ie sql.InternalExecutor
	c  kvtenant.Connector
}

// StartHotRangesLoggingScheduler starts the capture index usage statistics logging scheduler.
func StartHotRangesLoggingScheduler(
	ctx context.Context,
	stopper *stop.Stopper,
	tenantConnect kvtenant.Connector,
	ie sql.InternalExecutor,
) {
	scheduler := hotRangesDetailsLoggingScheduler{
		ie: ie,
		c:  tenantConnect,
	}
	scheduler.start(ctx, stopper)
}

func (s *hotRangesDetailsLoggingScheduler) start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "hot-ranges-struct-logging", func(ctx context.Context) {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ticker.C:
				resp, err := s.c.HotRangesV2(ctx, &serverpb.HotRangesRequest{})
				if err != nil {
					continue
				}
				var ranges = make([]eventpb.Ranges, len(resp.Ranges))
				for idx, r := range resp.Ranges {
					ranges[idx] = eventpb.Ranges{
						RangeID:      int64(r.RangeID),
						Qps:          r.QPS,
						DatabaseName: r.DatabaseName,
						SchemaName:   r.SchemaName,
						TableName:    r.TableName,
						IndexName:    r.IndexName,
					}
				}
				var hrs logpb.EventPayload = &eventpb.HotRangesStats{Ranges: ranges}
				log.StructuredEvent(ctx, hrs)
			}
		}
	})
}
