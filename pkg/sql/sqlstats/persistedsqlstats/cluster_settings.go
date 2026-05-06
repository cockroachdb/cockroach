// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/errors"
	"github.com/robfig/cron/v3"
)

// SQLStatsFlushInterval is the cluster setting that controls how often the SQL
// stats are flushed to system table.
var SQLStatsFlushInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.stats.flush.interval",
	"the interval at which SQL execution statistics are flushed to disk, "+
		"this value must be less than or equal to 1 hour",
	time.Minute*10,
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
	settings.WithPublic)

// SQLStatsFlushBatchSize is the cluster setting that controls how many
// rows are inserted per upsert during a sql stats flush.
var SQLStatsFlushBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.flush.batch_size",
	"the number of rows to flush per upsert",
	10,
	settings.NonNegativeInt)

// SQLStatsFlushCoordinatedBatchSize controls how many rows are written per
// UPSERT in a coordinated flush. Coordinated cycles work on cluster-wide
// merged stats, so they need a larger batch than per-node flushes to keep
// round-trip count manageable.
var SQLStatsFlushCoordinatedBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.flush.coordinated.batch_size",
	"the number of rows to flush per upsert in a coordinated SQL stats flush",
	500,
	settings.PositiveInt,
)

// MinimumInterval is the cluster setting that controls the minimum interval
// between each flush operation. If flush operations get triggered faster
// than what is allowed by this setting, (e.g. when too many fingerprints are
// generated in a short span of time, which in turn cause memory pressure), the
// flush operation will be aborted.
var MinimumInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.stats.flush.minimum_interval",
	"the minimum interval that SQL stats can be flushes to disk. If a "+
		"flush operation starts within less than the minimum interval, the flush "+
		"operation will be aborted",
	0,
)

// DiscardInMemoryStatsWhenFlushDisabled is the cluster setting that allows the
// older in-memory SQL stats to be discarded when flushing to persisted tables
// is disabled.
var DiscardInMemoryStatsWhenFlushDisabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.flush.force_cleanup.enabled",
	"if set, older SQL stats are discarded periodically when flushing to "+
		"persisted tables is disabled",
	false,
)

// SQLStatsFlushEnabled is the cluster setting that controls if the sqlstats
// subsystem persists the statistics into system table.
var SQLStatsFlushEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.flush.enabled",
	"if set, SQL execution statistics are periodically flushed to disk",
	true, /* defaultValue */
	settings.WithPublic)

// SQLStatsFlushCoordinatedEnabled opts a cluster into coordinated SQL
// stats flushes. When true, a singleton job drives all flushes and the
// per-node flush loops idle. See CoordinatedFlushEnabled for the
// effective predicate.
var SQLStatsFlushCoordinatedEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.flush.coordinated.enabled",
	"if set, a single coordinator job drives SQL stats flushes for the entire "+
		"cluster instead of each node flushing independently",
	false, /* defaultValue */
	settings.WithPublic)

// CoordinatedFlushEnabled returns whether the singleton coordinator owns
// the write path. Both the coordinator job and the per-node flush loop
// consult it so they stay in agreement.
//
// Coordinated mode collapses fingerprints across nodes into a single row,
// which is incompatible with gateway-node attribution; we disable
// ourselves in that mode regardless of the opt-in setting.
//
// TODO(kyle.wong): replace clusterversion.Latest with a dedicated key
// minted alongside the streaming DrainSqlStats RPC. Latest is overly
// conservative but safe.
func CoordinatedFlushEnabled(ctx context.Context, st *cluster.Settings) bool {
	if !st.Version.IsActive(ctx, clusterversion.Latest) {
		return false
	}
	if sqlstats.GatewayNodeEnabled.Get(&st.SV) {
		return false
	}
	return SQLStatsFlushCoordinatedEnabled.Get(&st.SV)
}

// SQLStatsFlushJitter specifies the jitter fraction on the interval between
// attempts to flush SQL Stats.
//
// [(1 - SQLStatsFlushJitter) * SQLStatsFlushInterval),
//
//	(1 + SQLStatsFlushJitter) * SQLStatsFlushInterval)]
var SQLStatsFlushJitter = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.stats.flush.jitter",
	"jitter fraction on the duration between sql stats flushes",
	0.15,
	settings.Fraction,
)

// SQLStatsMaxPersistedRows specifies maximum number of rows that will be
// retained in system.statement_statistics and system.transaction_statistics.
var SQLStatsMaxPersistedRows = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.persisted_rows.max",
	"maximum number of rows of statement and transaction statistics that "+
		"will be persisted in the system tables before compaction begins",
	1000000, /* defaultValue */
	settings.WithPublic)

// SQLStatsCleanupRecurrence is the cron-tab string specifying the recurrence
// for SQL Stats cleanup job.
var SQLStatsCleanupRecurrence = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"sql.stats.cleanup.recurrence",
	"cron-tab recurrence for SQL Stats cleanup job",
	"@hourly", /* defaultValue */
	settings.WithValidateString(func(_ *settings.Values, s string) error {
		if _, err := cron.ParseStandard(s); err != nil {
			return errors.Wrap(err, "invalid cron expression")
		}
		return nil
	}),
	settings.WithPublic,
)

// SQLStatsAggregationInterval is an alias for sqlstats.SQLStatsAggregationInterval.
var SQLStatsAggregationInterval = sqlstats.SQLStatsAggregationInterval

// CompactionJobRowsToDeletePerTxn is the cluster setting that controls
// how many rows in the statement/transaction_statistics tables gets deleted
// per transaction in the Automatic SQL Stats Compaction Job.
var CompactionJobRowsToDeletePerTxn = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.cleanup.rows_to_delete_per_txn",
	"number of rows the compaction job deletes from system table per iteration",
	10000,
	settings.NonNegativeInt,
)

// sqlStatsLimitTableSizeEnabled is the cluster setting that enables the
// sql stats system tables to grow past the number of rows set by
// sql.stats.persisted_row.max.
var sqlStatsLimitTableSizeEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.limit_table_size.enabled",
	"controls whether we allow statement and transaction statistics tables "+
		"to grow past sql.stats.persisted_rows.max",
	true,
)

// SQLStatsLimitTableCheckInterval is the cluster setting the controls what
// interval the check is done if the statement and transaction statistics
// tables have grown past the sql.stats.persisted_rows.max.
var SQLStatsLimitTableCheckInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.stats.limit_table_size_check.interval",
	"controls what interval the check is done if the statement and "+
		"transaction statistics tables have grown past sql.stats.persisted_rows.max",
	1*time.Hour,
)
