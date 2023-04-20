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

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ReportTopHottestRanges limits the number of ranges to be reported per iteration
const ReportTopHottestRanges = 5

var TelemetryHotRangesStatsInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"server.telemetry.hot_ranges_stats.interval",
	"the time interval to log hot ranges stats",
	4*time.Hour,
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

// hotRangesLoggingScheduler is responsible for logging index usage stats
// on a scheduled interval.
type hotRangesLoggingScheduler struct {
	ie      sql.InternalExecutor
	sServer serverpb.TenantStatusServer
	st      *cluster.Settings
}

// StartHotRangesLoggingScheduler starts the capture index usage statistics logging scheduler.
func StartHotRangesLoggingScheduler(
	ctx context.Context,
	stopper *stop.Stopper,
	sServer serverpb.TenantStatusServer,
	ie sql.InternalExecutor,
	st *cluster.Settings,
) error {
	scheduler := hotRangesLoggingScheduler{
		ie:      ie,
		sServer: sServer,
		st:      st,
	}

	return scheduler.start(ctx, stopper)
}

func (s *hotRangesLoggingScheduler) start(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "hot-ranges-stats", func(ctx context.Context) {
		ticker := time.NewTicker(TelemetryHotRangesStatsInterval.Get(&s.st.SV))
		defer ticker.Stop()

		TelemetryHotRangesStatsInterval.SetOnChange(&s.st.SV, func(ctx context.Context) {
			ticker.Reset(TelemetryHotRangesStatsInterval.Get(&s.st.SV))
		})

		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !logcrash.DiagnosticsReportingEnabled.Get(&s.st.SV) || !TelemetryHotRangesStatsEnabled.Get(&s.st.SV) {
					continue
				}
				resp, err := s.sServer.HotRangesV2(ctx, &serverpb.HotRangesRequest{PageSize: ReportTopHottestRanges})
				if err != nil {
					log.Warningf(ctx, "failed to get hot ranges: %s", err)
					continue
				}
				var events []logpb.EventPayload
				ts := timeutil.Now().UnixNano()

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
						CommonEventDetails: logpb.CommonEventDetails{
							Timestamp: ts,
						},
					}
					events = append(events, hrEvent)
				}
				logutil.LogEventsWithDelay(ctx, events, stopper, TelemetryHotRangesStatsLoggingDelay.Get(&s.st.SV))
			}
		}
	})
}
