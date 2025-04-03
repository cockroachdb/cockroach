// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package structlogging

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
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
	ie          sql.InternalExecutor
	sServer     serverpb.TenantStatusServer
	st          *cluster.Settings
	stopper     *stop.Stopper
	job         *jobs.Job
	multiTenant bool
}

// StartHotRangesLoggingScheduler starts the capture index usage statistics logging scheduler.
func StartHotRangesLoggingScheduler(
	ctx context.Context,
	stopper *stop.Stopper,
	sServer serverpb.TenantStatusServer,
	ie sql.InternalExecutor,
	st *cluster.Settings,
	ti *tenantcapabilities.Entry,
) error {
	multiTenant := ti != nil && !ti.TenantID.IsSystem()
	scheduler := hotRangesLoggingScheduler{
		ie:          ie,
		sServer:     sServer,
		st:          st,
		stopper:     stopper,
		multiTenant: multiTenant,
	}

	if multiTenant {
		return scheduler.startJob(ctx, stopper)
	}

	return scheduler.startTask(ctx, stopper)
}

func (s *hotRangesLoggingScheduler) startTask(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "hot-ranges-stats", func(ctx context.Context) {
		err := s.start(ctx, stopper)
		log.Warningf(ctx, "hot ranges stats logging scheduler stopped: %s", err)
	})
}

func (s *hotRangesLoggingScheduler) startJob(ctx context.Context, stopper *stop.Stopper) error {
	jobs.RegisterConstructor(
		jobspb.TypeHotRangesLogger,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &hotRangesLoggingScheduler{job: job}
		},
		jobs.DisablesTenantCostControl,
	)
	return nil
}

func (s *hotRangesLoggingScheduler) start(ctx context.Context, stopper *stop.Stopper) error {
	intervalChangedChan := make(chan struct{})
	// We have to register this callback first. Otherwise we may run into
	// an unlikely but possible scenario where we've started the ticker,
	// and the setting is changed before we register the callback and the
	// ticker will not be reset to the new value.
	TelemetryHotRangesStatsInterval.SetOnChange(&s.st.SV, func(ctx context.Context) {
		intervalChangedChan <- struct{}{}
	})

	ticker := time.NewTicker(TelemetryHotRangesStatsInterval.Get(&s.st.SV))

	for {
		select {
		case <-stopper.ShouldQuiesce():
			return nil
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.maybeLogHotRanges(ctx, stopper)
		case <-intervalChangedChan:
			ticker.Reset(TelemetryHotRangesStatsInterval.Get(&s.st.SV))
		}
	}
}

// maybeLogHotRanges is a small helper function which couples the
// functionality of checking whether to log and logging.
func (s *hotRangesLoggingScheduler) maybeLogHotRanges(ctx context.Context, stopper *stop.Stopper) {
	if s.shouldLog() {
		s.logHotRanges(ctx, stopper)
	}
}

// shouldLog checks the below conditions to see whether it should emit logs.
//   - Is the cluster setting server.telemetry.hot_ranges_stats.enabled true?
func (s *hotRangesLoggingScheduler) shouldLog() bool {
	return TelemetryHotRangesStatsEnabled.Get(&s.st.SV)
}

// logHotRanges collects the hot ranges from this node's status server and
// sends them to the TELEMETRY log channel.
func (s *hotRangesLoggingScheduler) logHotRanges(ctx context.Context, stopper *stop.Stopper) {
	req := &serverpb.HotRangesRequest{}

	// if we are running in single tenant mode, only log the ranges on the status server.
	if !s.multiTenant {
		req.Nodes = []string{"local"}
		req.PageSize = ReportTopHottestRanges
	}

	resp, err := s.sServer.HotRangesV2(ctx, req)
	if err != nil {
		log.Warningf(ctx, "failed to get hot ranges: %s", err)
		return
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
}
