// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scheduledloggingjobs

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
	"github.com/cockroachdb/errors"
	"github.com/robfig/cron/v3"
)

// CaptureIndexUsageStatsInterval is the interval at which index
// usage stats are exported to the telemetry logging channel.
var CaptureIndexUsageStatsInterval = settings.RegisterValidatedStringSetting(
	settings.TenantWritable,
	"sql.capture_index_usage_stats.telemetry.interval",
	"cron-tab interval to capture index usage statistics to the telemetry logging channel",
	"0 */8 * * *", /* defaultValue */
	func(_ *settings.Values, s string) error {
		if _, err := cron.ParseStandard(s); err != nil {
			return errors.Wrap(err, "invalid cron expression")
		}
		return nil
	},
).WithPublic()

const captureIndexStatsScheduleLabel = "capture-index-stats-telemetry"

// CaptureIndexUsageStatsResumeFunc is the resume callback function for the
// capture index usage stats job.
func CaptureIndexUsageStatsResumeFunc(
	ctx context.Context, ie sqlutil.InternalExecutor,
) ([]eventpb.EventPayload, error) {
	log.Infof(ctx, "starting capture of index usage stats job")

	// Get all database names.
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
		return nil, err
	}

	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { err = errors.CombineErrors(err, it.Close()) }()
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row while capturing index usage stats")
		}
		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns, received %d while capturing index usage stats", expectedNumDatums, row.Len())
		}

		databaseName := string(tree.MustBeDString(row[0]))
		allDatabaseNames = append(allDatabaseNames, databaseName)
	}

	// Capture index usage statistics for each database.
	//var collectedCapturedIndexStats eventpb.CollectedCapturedIndexUsageStats
	expectedNumDatums = 10
	var allEvents []eventpb.EventPayload
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

		it, err = ie.QueryIteratorEx(
			ctx,
			"capture-index-usage-stats",
			nil,
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			stmt,
		)
		if err != nil {
			return nil, err
		}

		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			var row tree.Datums
			if row = it.Cur(); row == nil {
				return nil, errors.New("unexpected null row while capturing index usage stats")
			}

			if row.Len() != expectedNumDatums {
				return nil, errors.Newf("expected %d columns, received %d while capturing index usage stats", expectedNumDatums, row.Len())
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

			allEvents = append(allEvents, capturedIndexStats)
		}
		err = it.Close()
		if err != nil {
			return nil, err
		}
	}
	return allEvents, nil
}

// CreateCaptureIndexUsageStatsScheduledJob creates the scheduled job to
// capture index usage stats. Intended to be used by the LoggingJobsController.
func CreateCaptureIndexUsageStatsScheduledJob(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn, cs *cluster.Settings,
) error {

	scheduleDetails := jobspb.ScheduleDetails{
		Wait:    jobspb.ScheduleDetails_WAIT,
		OnError: jobspb.ScheduleDetails_RETRY_SCHED,
	}

	execArgs := &CaptureIndexUsageStatsExecutionArgs{}

	_, err := CreateLoggingScheduleJob(
		ctx, ie, txn,
		captureIndexStatsScheduleLabel,
		CaptureIndexUsageStatsInterval.Get(&cs.SV),
		tree.CaptureIndexUsageStatsExecutor,
		scheduleDetails,
		execArgs)
	if err != nil {
		return err
	}
	return nil
}
