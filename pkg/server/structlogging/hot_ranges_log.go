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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// ReportTopHottestRanges limits the number of ranges to be reported per iteration
const ReportTopHottestRanges = 5

var TelemetryHotRangesStatsInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"server.telemetry.hot_ranges_stats.interval",
	"the time interval to log hot ranges stats",
	24*time.Hour,
	settings.NonNegativeDuration,
)

var TelemetryHotRangesStatsEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"server.telemetry.hot_ranges_stats.enabled",
	"enable/disable capturing hot ranges statistics to the telemetry logging channel",
	true,
)

var TelemetryHotRangesStatsLoggingDelay = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"server.telemetry.hot_ranges_stats.logging_delay",
	"the time delay between emitting individual hot ranges stats logs",
	1*time.Second,
	settings.NonNegativeDuration,
)

// hotRangesDetailsLoggingScheduler is responsible for logging index usage stats
// on a scheduled interval.
type hotRangesDetailsLoggingScheduler struct {
	ie sql.InternalExecutor
	c  kvtenant.Connector
	st *cluster.Settings
}

// StartHotRangesLoggingScheduler starts the capture index usage statistics logging scheduler.
func StartHotRangesLoggingScheduler(
	ctx context.Context,
	stopper *stop.Stopper,
	tenantConnect kvtenant.Connector,
	ie sql.InternalExecutor,
	st *cluster.Settings,
) {
	scheduler := hotRangesDetailsLoggingScheduler{
		ie: ie,
		c:  tenantConnect,
		st: st,
	}
	scheduler.start(ctx, stopper)
}

func (s *hotRangesDetailsLoggingScheduler) start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "hot-ranges-stats", func(ctx context.Context) {
		ticker := time.NewTicker(TelemetryHotRangesStatsInterval.Get(&s.st.SV))
		defer ticker.Stop()

		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ticker.C:
				if !TelemetryHotRangesStatsEnabled.Get(&s.st.SV) {
					continue
				}
				resp, err := s.c.HotRangesV2(ctx, &serverpb.HotRangesRequest{PageSize: ReportTopHottestRanges})
				if err != nil {
					log.Warningf(ctx, "failed to get hot ranges: %s", err)
					continue
				}
				var events []logpb.EventPayload

				for _, r := range resp.Ranges {
					hrEvent := &eventpb.HotRangesStats{
						RangeID:             int64(r.RangeID),
						Qps:                 r.QPS,
						DatabaseName:        r.DatabaseName,
						SchemaName:          r.SchemaName,
						TableName:           r.TableName,
						IndexName:           r.IndexName,
						CPUTimePerSecond:    r.CPUTimePerSecond,
						ReadBytesPerSecond:  r.ReadBytesPerSecond,
						WriteBytesPerSecond: r.WriteBytesPerSecond,
						ReadsPerSecond:      r.ReadsPerSecond,
						WritesPerSecond:     r.WritesPerSecond,
						LeaseholderNodeID:   int32(r.LeaseholderNodeID),
					}
					events = append(events, hrEvent)
				}
				logEventsWithDelay(ctx, events, stopper, TelemetryHotRangesStatsLoggingDelay.Get(&s.st.SV))
			}
		}
	})
}

func logEventsWithDelay(
	ctx context.Context, events []logpb.EventPayload, stopper *stop.Stopper, delay time.Duration,
) {
	// Log the first event immediately.
	timer := time.NewTimer(0 * time.Second)
	defer timer.Stop()
	for len(events) > 0 {
		select {
		case <-stopper.ShouldQuiesce():
			return
		case <-timer.C:
			event := events[0]
			log.StructuredEvent(ctx, event)
			events = events[1:]
			// Apply a delay to subsequent events.
			timer.Reset(delay)
		}
	}
}
