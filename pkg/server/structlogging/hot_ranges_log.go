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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ReportTopHottestRanges limits the number of ranges to be reported per iteration
var ReportTopHottestRanges int32 = 5

// CheckInterval is the interval at which the system checks
// whether or not to log the hot ranges.
var CheckInterval = time.Minute

// TestLoopChannel triggers the hot ranges logging loop to start again.
// It's useful in the context of a test, where we don't want to wait
// for whatever the last time the interval was.
var TestLoopChannel = make(chan struct{})

var TelemetryHotRangesStatsInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.telemetry.hot_ranges_stats.interval",
	"the time interval to log hot ranges stats",
	4*time.Hour,
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
)

// TelemetryHotRangesStatsCPUThreshold defines the cpu duration
// per second which needs to be exceeded for the system to automatically
// log the hot ranges. It tracks the reasoning that the kv layer
// uses to determine when to begin sampling reads for a given
// range in the keyspace, more information found where the cluster
// setting SplitByLoadCPUThreshold is defined.
var TelemetryHotRangesStatsCPUThreshold = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"server.telemetry.hot_ranges_stats.cpu_threshold",
	"the cpu time over which the system will automatically begin logging hot ranges",
	time.Second/4,
)

type HotRangeGetter interface {
	HotRangesV2(ctx context.Context, req *serverpb.HotRangesRequest) (*serverpb.HotRangesResponseV2, error)
}

// hotRangesLoggingScheduler is responsible for logging index usage stats
// on a scheduled interval.
type hotRangesLoggingScheduler struct {
	sServer     HotRangeGetter
	st          *cluster.Settings
	stopper     *stop.Stopper
	job         *jobs.Job
	multiTenant bool
	lastLogged  time.Time
}

// StartHotRangesLoggingScheduler starts the hot range log task
// or job.
//
// For system tenants, or single tenant deployments, it runs as
// a task on each node, logging only the ranges on the node in
// which it runs. For app tenants in a multi-tenant deployment,
// it runs on a single node in the sql cluster, applying a fanout
// to the kv layer to collect the hot ranges from all nodes.
func StartHotRangesLoggingScheduler(
	ctx context.Context,
	stopper *stop.Stopper,
	sServer HotRangeGetter,
	st *cluster.Settings,
	ti *tenantcapabilities.Entry,
) error {
	multiTenant := ti != nil && ti.TenantID.IsSet() && !ti.TenantID.IsSystem()
	scheduler := hotRangesLoggingScheduler{
		sServer:     sServer,
		st:          st,
		stopper:     stopper,
		multiTenant: multiTenant,
		lastLogged:  timeutil.Now(),
	}

	if multiTenant {
		return scheduler.startJob()
	}

	return scheduler.startTask(ctx, stopper)
}

// startTask is for usage in a system-tenant or non-multi-tenant
// installation.
func (s *hotRangesLoggingScheduler) startTask(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "hot-ranges-stats", func(ctx context.Context) {
		s.start(ctx, stopper)
	})
}

func (s *hotRangesLoggingScheduler) startJob() error {
	jobs.RegisterConstructor(
		jobspb.TypeHotRangesLogger,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &hotRangesLoggingScheduler{job: job}
		},
		jobs.DisablesTenantCostControl,
	)
	return nil
}

func (s *hotRangesLoggingScheduler) start(ctx context.Context, stopper *stop.Stopper) {
	for {
		ci := CheckInterval
		if s.multiTenant {
			ci *= 5
		}
		select {
		case <-stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		case <-time.After(ci):
			s.maybeLogHotRanges(ctx, stopper)
		case <-TestLoopChannel:
			continue
		}
	}
}

// maybeLogHotRanges is a small helper function which couples the
// functionality of checking whether to log and logging.
func (s *hotRangesLoggingScheduler) maybeLogHotRanges(ctx context.Context, stopper *stop.Stopper) {
	if s.shouldLog(ctx) {
		s.logHotRanges(ctx, stopper)
		s.lastLogged = timeutil.Now()
	}
}

// shouldLog checks the below conditions to see whether it
// should emit logs.
//
//		To return true, we verify that both:
//		 - The logging setting is enabled.
//	   - One of the following conditions is met:
//		   -- It's been greater than the log interval since we last logged.
//		   -- One of the replicas see exceeds our cpu threshold.
func (s *hotRangesLoggingScheduler) shouldLog(ctx context.Context) bool {
	enabled := TelemetryHotRangesStatsEnabled.Get(&s.st.SV)
	if !enabled {
		return false
	}

	logInterval := TelemetryHotRangesStatsInterval.Get(&s.st.SV)
	if timeutil.Since(s.lastLogged) > logInterval {
		return true
	}

	// Getting the hot ranges with the statsOnly flag will
	// ensure the call doesn't touch the keyspace. Therefore
	// drastically lightening the overhead of fetching them.
	resp, err := s.getHotRanges(context.Background(), true)
	if err != nil {
		log.Warningf(ctx, "failed to get hot ranges: %s", err)
		return false
	}
	cpuThreshold := TelemetryHotRangesStatsCPUThreshold.Get(&s.st.SV)
	return maxCPU(resp.Ranges) > cpuThreshold
}

func maxCPU(ranges []*serverpb.HotRangesResponseV2_HotRange) time.Duration {
	maxSeen := float64(0)
	for _, r := range ranges {
		if r.CPUTimePerSecond > maxSeen {
			maxSeen = r.CPUTimePerSecond
		}
	}
	return time.Duration(maxSeen)
}

// getHotRanges is a simple utility function for making a hot ranges
// request to the status server. It can be used to fetch only the
// stats for ranges requested, or everything. It also determines
// whether to limit the request to only the local node, or to
// issue a fanout for multi-tenant apps.
func (s *hotRangesLoggingScheduler) getHotRanges(
	ctx context.Context, statsOnly bool,
) (*serverpb.HotRangesResponseV2, error) {
	req := &serverpb.HotRangesRequest{
		PerNodeLimit: ReportTopHottestRanges,
		StatsOnly:    statsOnly,
	}

	// if we are running in single tenant mode, only log the ranges on the status server.
	if !s.multiTenant {
		req.Nodes = []string{"local"}
	}

	return s.sServer.HotRangesV2(ctx, req)
}

// logHotRanges collects the hot ranges from this node's status server and
// sends them to the HEALTH log channel.
func (s *hotRangesLoggingScheduler) logHotRanges(ctx context.Context, stopper *stop.Stopper) {
	resp, err := s.getHotRanges(ctx, false)
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
