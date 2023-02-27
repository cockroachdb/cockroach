// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
	"github.com/robfig/cron/v3"
)

// SQLStatsFlushInterval is the cluster setting that controls how often the SQL
// stats are flushed to system table.
var SQLStatsFlushInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.stats.flush.interval",
	"the interval at which SQL execution statistics are flushed to disk, "+
		"this value must be less than or equal to 1 hour",
	time.Minute*10,
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
).WithPublic()

// MinimumInterval is the cluster setting that controls the minimum interval
// between each flush operation. If flush operations get triggered faster
// than what is allowed by this setting, (e.g. when too many fingerprints are
// generated in a short span of time, which in turn cause memory pressure), the
// flush operation will be aborted.
var MinimumInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.stats.flush.minimum_interval",
	"the minimum interval that SQL stats can be flushes to disk. If a "+
		"flush operation starts within less than the minimum interval, the flush "+
		"operation will be aborted",
	0,
	settings.NonNegativeDuration,
)

// DiscardInMemoryStatsWhenFlushDisabled is the cluster setting that allows the
// older in-memory SQL stats to be discarded when flushing to persisted tables
// is disabled.
var DiscardInMemoryStatsWhenFlushDisabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.stats.flush.force_cleanup.enabled",
	"if set, older SQL stats are discarded periodically when flushing to "+
		"persisted tables is disabled",
	false,
)

// SQLStatsFlushEnabled is the cluster setting that controls if the sqlstats
// subsystem persists the statistics into system table.
var SQLStatsFlushEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.stats.flush.enabled",
	"if set, SQL execution statistics are periodically flushed to disk",
	true, /* defaultValue */
).WithPublic()

// SQLStatsFlushJitter specifies the jitter fraction on the interval between
// attempts to flush SQL Stats.
//
// [(1 - SQLStatsFlushJitter) * SQLStatsFlushInterval),
//
//	(1 + SQLStatsFlushJitter) * SQLStatsFlushInterval)]
var SQLStatsFlushJitter = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"sql.stats.flush.jitter",
	"jitter fraction on the duration between sql stats flushes",
	0.15,
	func(f float64) error {
		if f < 0 || f > 1 {
			return errors.Newf("%f is not in [0, 1]", f)
		}
		return nil
	},
)

// SQLStatsMaxPersistedRows specifies maximum number of rows that will be
// retained in system.statement_statistics and system.transaction_statistics.
var SQLStatsMaxPersistedRows = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.stats.persisted_rows.max",
	"maximum number of rows of statement and transaction"+
		" statistics that will be persisted in the system tables",
	1000000, /* defaultValue */
).WithPublic()

// SQLStatsCleanupRecurrence is the cron-tab string specifying the recurrence
// for SQL Stats cleanup job.
var SQLStatsCleanupRecurrence = settings.RegisterValidatedStringSetting(
	settings.TenantWritable,
	"sql.stats.cleanup.recurrence",
	"cron-tab recurrence for SQL Stats cleanup job",
	"@hourly", /* defaultValue */
	func(_ *settings.Values, s string) error {
		if _, err := cron.ParseStandard(s); err != nil {
			return errors.Wrap(err, "invalid cron expression")
		}
		return nil
	},
).WithPublic()

// SQLStatsAggregationInterval is the cluster setting that controls the aggregation
// interval for stats when we flush to disk.
var SQLStatsAggregationInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.stats.aggregation.interval",
	"the interval at which we aggregate SQL execution statistics upon flush, "+
		"this value must be greater than or equal to sql.stats.flush.interval",
	time.Hour,
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
)

// CompactionJobRowsToDeletePerTxn is the cluster setting that controls
// how many rows in the statement/transaction_statistics tables gets deleted
// per transaction in the Automatic SQL Stats Compaction Job.
var CompactionJobRowsToDeletePerTxn = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.stats.cleanup.rows_to_delete_per_txn",
	"number of rows the compaction job deletes from system table per iteration",
	10000,
	settings.NonNegativeInt,
)
