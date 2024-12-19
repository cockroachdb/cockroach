// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ReportTopHottestRanges limits the number of ranges to be reported per iteration
const ReportTopHottestRanges = 5

var TelemetryHotRangesStatsInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.telemetry.hot_ranges_stats.interval",
	"the time interval to log hot ranges stats",
	4*time.Hour,
	settings.NonNegativeDuration,
)

var TelemetryHotRangesStatsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.telemetry.hot_ranges_stats.enabled",
	"enable/disable capturing hot ranges statistics to the telemetry logging channel",
	true,
)

var TelemetryHotRangesStatsLoggingDelay = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
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
		intervalChangedChan := make(chan struct{})
		// We have to register this callback first. Otherwise we may run into
		// an unlikely but possible scenario where we've started the ticker,
		// and the setting is changed before we register the callback and the
		// ticker will not be reset to the new value.
		TelemetryHotRangesStatsInterval.SetOnChange(&s.st.SV, func(ctx context.Context) {
			intervalChangedChan <- struct{}{}
		})

		ticker := time.NewTicker(TelemetryHotRangesStatsInterval.Get(&s.st.SV))
		defer ticker.Stop()

		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !TelemetryHotRangesStatsEnabled.Get(&s.st.SV) {
					continue
				}
				resp, err := s.sServer.HotRangesV2(ctx,
					&serverpb.HotRangesRequest{NodeID: "local", PageSize: ReportTopHottestRanges})
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
						Databases:           r.Databases,
						Tables:              r.Tables,
						Indexes:             r.Indexes,
						SchemaName:          r.SchemaName,
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

			case <-intervalChangedChan:
				ticker.Reset(TelemetryHotRangesStatsInterval.Get(&s.st.SV))
			}
		}
	})
}
