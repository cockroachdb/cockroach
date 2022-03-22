// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scheduledlogging

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var telemetryCaptureIndexUsageStatsEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.telemetry.capture_index_usage_stats.enabled",
	"enable/disable capturing index usage statistics to the telemetry logging channel",
	true,
)

var telemetryCaptureIndexUsageStatsInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"sql.telemetry.capture_index_usage_stats.interval",
	"the scheduled interval time between capturing index usage statistics when capturing index usage statistics is enabled",
	8*time.Hour,
	settings.NonNegativeDuration,
)

var telemetryCaptureIndexUsageStatsStatusCheckEnabledInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"sql.telemetry.capture_index_usage_stats.check_enabled_interval",
	"the scheduled interval time between checks to see if index usage statistics has been enabled",
	10*time.Minute,
	settings.NonNegativeDuration,
)

var telemetryCaptureIndexUsageStatsLoggingDelay = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"sql.telemetry.capture_index_usage_stats.logging_delay",
	"the time delay between emitting individual index usage stats logs, this is done to "+
		"mitigate the log-line limit of 10 logs per second on the telemetry pipeline",
	500*time.Millisecond,
	settings.NonNegativeDuration,
)

// CaptureIndexUsageStatsTestingKnobs provides hooks and knobs for unit tests.
type CaptureIndexUsageStatsTestingKnobs struct {
	// getLoggingDuration allows tests to override the duration of the index
	// usage stats logging operation.
	getLoggingDuration func() time.Duration
	// getOverlapDuration allows tests to override the duration until the next
	// scheduled interval in the case that the logging duration exceeds the
	// default scheduled interval duration.
	getOverlapDuration func() time.Duration
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*CaptureIndexUsageStatsTestingKnobs) ModuleTestingKnobs() {}

// CaptureIndexUsageStatsLoggingScheduler is responsible for logging index usage stats
// on a scheduled interval.
type CaptureIndexUsageStatsLoggingScheduler struct {
	db                      *kv.DB
	st                      *cluster.Settings
	ie                      sqlutil.InternalExecutor
	knobs                   *CaptureIndexUsageStatsTestingKnobs
	currentCaptureStartTime time.Time
}

func (s *CaptureIndexUsageStatsLoggingScheduler) getLoggingDuration() time.Duration {
	if s.knobs != nil && s.knobs.getLoggingDuration != nil {
		return s.knobs.getLoggingDuration()
	}
	return timeutil.Since(s.currentCaptureStartTime)
}

func (s *CaptureIndexUsageStatsLoggingScheduler) durationOnOverlap() time.Duration {
	if s.knobs != nil && s.knobs.getOverlapDuration != nil {
		return s.knobs.getOverlapDuration()
	}
	// If the logging duration overlaps into the next scheduled interval, start
	// the next scheduled interval immediately instead of waiting.
	return 0 * time.Second
}

func (s *CaptureIndexUsageStatsLoggingScheduler) durationUntilNextInterval() time.Duration {
	// If telemetry is disabled, return the disabled interval duration.
	if !telemetryCaptureIndexUsageStatsEnabled.Get(&s.st.SV) {
		return telemetryCaptureIndexUsageStatsStatusCheckEnabledInterval.Get(&s.st.SV)
	}
	// If the previous logging operation took longer than or equal to the set
	// schedule interval, schedule the next interval immediately.
	if s.getLoggingDuration() >= telemetryCaptureIndexUsageStatsInterval.Get(&s.st.SV) {
		return s.durationOnOverlap()
	}
	// Otherwise, schedule the next interval normally.
	return telemetryCaptureIndexUsageStatsInterval.Get(&s.st.SV)
}

// Start starts the capture index usage statistics logging scheduler.
func Start(
	ctx context.Context,
	stopper *stop.Stopper,
	db *kv.DB,
	cs *cluster.Settings,
	ie sqlutil.InternalExecutor,
	knobs *CaptureIndexUsageStatsTestingKnobs,
) {
	scheduler := CaptureIndexUsageStatsLoggingScheduler{
		db:    db,
		st:    cs,
		ie:    ie,
		knobs: knobs,
	}
	scheduler.start(ctx, stopper)
}

func (s *CaptureIndexUsageStatsLoggingScheduler) start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "capture-index-usage-stats", func(ctx context.Context) {
		// Start the scheduler immediately.
		for timer := time.NewTimer(0 * time.Second); ; timer.Reset(s.durationUntilNextInterval()) {
			select {
			case <-stopper.ShouldQuiesce():
				timer.Stop()
				return
			case <-timer.C:
				s.currentCaptureStartTime = timeutil.Now()
				if !telemetryCaptureIndexUsageStatsEnabled.Get(&s.st.SV) {
					continue
				}

				err := captureIndexUsageStats(ctx, s.ie, stopper, telemetryCaptureIndexUsageStatsLoggingDelay.Get(&s.st.SV))
				if err != nil {
					log.Warningf(ctx, "error capturing index usage stats: %+v", err)
				}
			}
		}
	})
}

func captureIndexUsageStats(
	ctx context.Context,
	ie sqlutil.InternalExecutor,
	stopper *stop.Stopper,
	loggingDelay time.Duration,
) error {
	allDatabaseNames, err := getAllDatabaseNames(ctx, ie)
	if err != nil {
		return err
	}

	// Capture index usage statistics for each database.
	var ok bool
	expectedNumDatums := 9
	var allCapturedIndexUsageStats []eventpb.EventPayload
	for _, databaseName := range allDatabaseNames {
		// Omit index usage statistics on the default databases 'system',
		// 'defaultdb', and 'postgres'.
		if databaseName == "system" || databaseName == "defaultdb" || databaseName == "postgres" {
			continue
		}
		stmt := fmt.Sprintf(`
		SELECT
		 ti.descriptor_name as table_name,
		 ti.descriptor_id as table_id,
		 ti.index_name,
		 ti.index_id,
		 ti.index_type,
		 ti.is_unique,
		 ti.is_inverted,
		 total_reads,
		 last_read
		FROM %s.crdb_internal.index_usage_statistics AS us
		JOIN %s.crdb_internal.table_indexes ti
		ON us.index_id = ti.index_id
		 AND us.table_id = ti.descriptor_id
		ORDER BY total_reads ASC;
	`, databaseName, databaseName)

		it, err := ie.QueryIteratorEx(
			ctx,
			"capture-index-usage-stats",
			nil,
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			stmt,
		)
		if err != nil {
			return err
		}

		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			var row tree.Datums
			if err != nil {
				return err
			}
			if row = it.Cur(); row == nil {
				return errors.New("unexpected null row while capturing index usage stats")
			}

			if row.Len() != expectedNumDatums {
				return errors.Newf("expected %d columns, received %d while capturing index usage stats", expectedNumDatums, row.Len())
			}

			tableName := tree.MustBeDString(row[0])
			tableID := tree.MustBeDInt(row[1])
			indexName := tree.MustBeDString(row[2])
			indexID := tree.MustBeDInt(row[3])
			indexType := tree.MustBeDString(row[4])
			isUnique := tree.MustBeDBool(row[5])
			isInverted := tree.MustBeDBool(row[6])
			totalReads := tree.MustBeDInt(row[7])
			lastRead := time.Time{}
			if row[8] != tree.DNull {
				lastRead = tree.MustBeDTimestampTZ(row[8]).Time
			}

			capturedIndexStats := &eventpb.CapturedIndexUsageStats{
				TableID:        uint32(roachpb.TableID(tableID)),
				IndexID:        uint32(roachpb.IndexID(indexID)),
				TotalReadCount: uint64(totalReads),
				LastRead:       lastRead.String(),
				DatabaseName:   databaseName,
				TableName:      string(tableName),
				IndexName:      string(indexName),
				IndexType:      string(indexType),
				IsUnique:       bool(isUnique),
				IsInverted:     bool(isInverted),
			}

			allCapturedIndexUsageStats = append(allCapturedIndexUsageStats, capturedIndexStats)
		}
		if err = it.Close(); err != nil {
			return err
		}
	}
	logIndexUsageStatsWithDelay(ctx, allCapturedIndexUsageStats, stopper, loggingDelay)
	return nil
}

// logIndexUsageStatsWithDelay logs an eventpb.EventPayload at each
// telemetryCaptureIndexUsageStatsLoggingDelay to avoid exceeding the 10
// log-line per second limit per node on the telemetry logging pipeline.
// Currently, this log-line limit is only shared with 1 other telemetry event,
// SampledQuery, which now has a logging frequency of 8 logs per second.
func logIndexUsageStatsWithDelay(
	ctx context.Context, events []eventpb.EventPayload, stopper *stop.Stopper, delay time.Duration,
) {

	// Log the first event immediately.
	timer := time.NewTimer(0 * time.Second)
	for len(events) > 0 {
		select {
		case <-stopper.ShouldQuiesce():
			timer.Stop()
			return
		case <-timer.C:
			event := events[0]
			log.StructuredEvent(ctx, event)
			events = events[1:]
			// Apply a delay to subsequent events.
			timer.Reset(delay)
		}
	}
	timer.Stop()
}

func getAllDatabaseNames(ctx context.Context, ie sqlutil.InternalExecutor) ([]string, error) {
	var allDatabaseNames []string
	var ok bool
	var expectedNumDatums = 1

	it, err := ie.QueryIteratorEx(
		ctx,
		"get-all-db-names",
		nil,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		`SELECT database_name FROM [SHOW DATABASES]`,
	)
	if err != nil {
		return []string{}, err
	}

	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { err = errors.CombineErrors(err, it.Close()) }()
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return []string{}, errors.New("unexpected null row while capturing index usage stats")
		}
		if row.Len() != expectedNumDatums {
			return []string{}, errors.Newf("expected %d columns, received %d while capturing index usage stats", expectedNumDatums, row.Len())
		}

		databaseName := string(tree.MustBeDString(row[0]))
		allDatabaseNames = append(allDatabaseNames, databaseName)
	}
	return allDatabaseNames, nil
}
