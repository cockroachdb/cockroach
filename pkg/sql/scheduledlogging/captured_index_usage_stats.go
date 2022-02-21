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
	"the time between capturing index usage statistics",
	8*time.Second,
	settings.NonNegativeDuration,
)

const captureIndexUsageStatsLoggingDelay = 5000 * time.Millisecond

// CaptureIndexUsageStatsLoggingScheduler is responsible for logging index usage stats
// on a scheduled interval.
type CaptureIndexUsageStatsLoggingScheduler struct {
	DB *kv.DB
	cs *cluster.Settings
	ie sqlutil.InternalExecutor
}

// StartCapturingIndexUsageStats creates a LoggingScheduler and starts all log
// emitters.
func StartCapturingIndexUsageStats(
	ctx context.Context,
	stopper *stop.Stopper,
	db *kv.DB,
	cs *cluster.Settings,
	ie sqlutil.InternalExecutor,
) {
	scheduler := CaptureIndexUsageStatsLoggingScheduler{
		DB: db,
		cs: cs,
		ie: ie,
	}
	scheduler.start(ctx, stopper)
}

func (s *CaptureIndexUsageStatsLoggingScheduler) start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "capture-index-usage-stats", func(ctx context.Context) {
		for timer := time.NewTimer(telemetryCaptureIndexUsageStatsInterval.Get(&s.cs.SV)); ; timer.Reset(
			telemetryCaptureIndexUsageStatsInterval.Get(&s.cs.SV)) {
			select {
			case <-stopper.ShouldQuiesce():
				timer.Stop()
				return
			case <-timer.C:
				if !telemetryCaptureIndexUsageStatsEnabled.Get(&s.cs.SV) {
					continue
				}

				// Note: the timer will only reset once capturing index usage stats is
				// complete.
				err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return captureIndexUsageStats(ctx, s.ie, stopper)
				})
				if err != nil {
					log.Errorf(ctx, "error capturing index usage stats: %+v", err)
				}
			}
		}
	})
}

// Emit implements the ScheduledLogEmitter interface.
func captureIndexUsageStats(
	ctx context.Context, ie sqlutil.InternalExecutor, stopper *stop.Stopper,
) error {
	allDatabaseNames, err := getAllDatabaseNames(ctx, ie)
	if err != nil {
		return err
	}

	// Capture index usage statistics for each database.
	var ok bool
	expectedNumDatums := 10
	var allCapturedIndexUsageStats []eventpb.EventPayload
	for _, databaseName := range allDatabaseNames {
		// Omit index usage statistics of the 'system' database.
		if databaseName == "system" {
			continue
		}
		stmt := fmt.Sprintf(`
		SELECT
		'%s' as database_name,
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
	`, databaseName, databaseName, databaseName)

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
			if row = it.Cur(); row == nil {
				return errors.New("unexpected null row while capturing index usage stats")
			}

			if row.Len() != expectedNumDatums {
				return errors.Newf("expected %d columns, received %d while capturing index usage stats", expectedNumDatums, row.Len())
			}

			databaseName := tree.MustBeDString(row[0])
			tableName := tree.MustBeDString(row[1])
			tableID := tree.MustBeDInt(row[2])
			indexName := tree.MustBeDString(row[3])
			indexID := tree.MustBeDInt(row[4])
			indexType := tree.MustBeDString(row[5])
			isUnique := tree.MustBeDBool(row[6])
			isInverted := tree.MustBeDBool(row[7])
			totalReads := uint64(tree.MustBeDInt(row[8]))
			lastRead := time.Time{}
			if row[9] != tree.DNull {
				lastRead = tree.MustBeDTimestampTZ(row[9]).Time
			}

			capturedIndexStats := &eventpb.CapturedIndexUsageStats{
				TableID:        uint32(roachpb.TableID(tableID)),
				IndexID:        uint32(roachpb.IndexID(indexID)),
				TotalReadCount: totalReads,
				LastRead:       lastRead.String(),
				DatabaseName:   string(databaseName),
				TableName:      string(tableName),
				IndexName:      string(indexName),
				IndexType:      string(indexType),
				IsUnique:       bool(isUnique),
				IsInverted:     bool(isInverted),
			}

			allCapturedIndexUsageStats = append(allCapturedIndexUsageStats, capturedIndexStats)
		}
		err = it.Close()
		if err != nil {
			return err
		}
	}
	logIndexUsageStatsWithDelay(ctx, allCapturedIndexUsageStats, stopper)
	return nil
}

// logIndexUsageStatsWithDelay logs a slice of eventpb.EventPayload at half
// second intervals (2 logs per second) to avoid exceeding the 10 log-line per
// second limit per node on the telemetry logging pipeline.
func logIndexUsageStatsWithDelay(
	ctx context.Context, events []eventpb.EventPayload, stopper *stop.Stopper,
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
			timer.Reset(captureIndexUsageStatsLoggingDelay)
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
