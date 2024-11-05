// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	math_rand "math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var (
	systemLogGCPeriod = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.log_gc.period",
		"the period at which log-like system tables are checked for old entries",
		time.Hour,
		settings.NonNegativeDuration,
		settings.WithPublic)

	systemLogGCLimit = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"server.log_gc.max_deletions_per_cycle",
		"the maximum number of entries to delete on each purge of log-like system tables",
		1000,
		settings.WithPublic)

	// rangeLogTTL is the TTL for rows in system.rangelog. If non zero, range log
	// entries are periodically garbage collected.
	rangeLogTTL = settings.RegisterDurationSetting(
		settings.SystemOnly,
		"server.rangelog.ttl",
		"if nonzero, entries in system.rangelog older than this duration are periodically purged",
		30*24*time.Hour, // 30 days
		settings.WithPublic)

	// eventLogTTL is the TTL for rows in system.eventlog. If non zero, event log
	// entries are periodically garbage collected.
	eventLogTTL = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.eventlog.ttl",
		"if nonzero, entries in system.eventlog older than this duration are periodically purged",
		90*24*time.Hour, // 90 days
		settings.WithPublic)

	webSessionPurgeTTL = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.web_session.purge.ttl",
		"if nonzero, entries in system.web_sessions older than this duration are periodically purged",
		time.Hour,
		settings.WithPublic)
)

// gcSystemLog deletes entries in the given system log table between
// timestampLowerBound and timestampUpperBound.
// The system log table is expected to have a "timestamp" column.
// It returns the timestampLowerBound to be used in the next iteration, number
// of rows affected and error (if any).
func gcSystemLog(
	ctx context.Context,
	sqlServer *SQLServer,
	opName redact.RedactableString,
	table, tsCol string,
	timestampLowerBound, timestampUpperBound time.Time,
	limit int64,
) (time.Time, int64, error) {
	var totalRowsAffected int64

	deleteStmt := fmt.Sprintf(
		`WITH d AS (
   DELETE FROM system.public.%[1]s
    WHERE %[2]s >= $1 AND %[2]s <= $2
    LIMIT $3
RETURNING %[2]s
)
SELECT count(1), max(%[2]s) FROM d`,
		lexbase.EscapeSQLIdent(table),
		lexbase.EscapeSQLIdent(tsCol),
	)

	for {
		var rowsAffected int64
		err := func() error {
			row, err := sqlServer.internalExecutor.QueryRowEx(
				ctx,
				opName,
				nil, /* txn */
				sessiondata.NodeUserSessionDataOverride,
				deleteStmt,
				timestampLowerBound,
				timestampUpperBound,
				limit,
			)
			if err != nil {
				return err
			}

			if row != nil {
				rowCount, ok := row[0].(*tree.DInt)
				if !ok {
					return errors.Errorf("row count is of unknown type %T", row[0])
				}
				if rowCount == nil {
					return errors.New("error parsing row count")
				}
				rowsAffected = int64(*rowCount)

				if rowsAffected > 0 {
					maxTimestamp, ok := row[1].(*tree.DTimestamp)
					if !ok {
						return errors.Errorf("timestamp is of unknown type %T", row[1])
					}
					if maxTimestamp == nil {
						return errors.New("error parsing timestamp")
					}
					timestampLowerBound = maxTimestamp.Time
				}
			}
			return nil
		}()

		totalRowsAffected += rowsAffected
		if err != nil {
			return timestampLowerBound, totalRowsAffected, err
		}

		if rowsAffected == 0 {
			return timestampUpperBound, totalRowsAffected, nil
		}
	}
}

// systemLogGCConfig has configurations for gc of systemlog.
type systemLogGCConfig struct {
	// onlySystemTenant indicates whether the cleanup for this
	// table should occur in secondary tenants or not.
	onlySystemTenant bool

	// table is the name of the system table.
	table string

	// timestampCol is the name of the timestamp column, used
	// as condition to cut old values.
	timestampCol string

	// ttl is the time to live for rows in systemlog table.
	ttl *settings.DurationSetting

	// timestampLowerBound is the timestamp below which rows are gc'ed.
	// It is maintained to avoid hitting tombstones during gc and is updated
	// after every gc run.
	timestampLowerBound time.Time
}

func runSystemLogGCForOneTable(
	ctx context.Context, sqlServer *SQLServer, st *cluster.Settings, gcConfig *systemLogGCConfig,
) (int64, error) {
	ttl := gcConfig.ttl.Get(&st.SV)
	if ttl == 0 {
		// GC disabled for this table. Do nothing.
		return 0, nil
	}

	opName := redact.Sprintf("%s-%s-gc", gcConfig.table, gcConfig.timestampCol)
	limit := systemLogGCLimit.Get(&st.SV)
	timestampUpperBound := timeutil.Unix(0, sqlServer.execCfg.Clock.PhysicalNow()-int64(ttl))
	newTimestampLowerBound, rowsAffected, err := gcSystemLog(
		ctx, sqlServer, opName,
		gcConfig.table, gcConfig.timestampCol,
		gcConfig.timestampLowerBound,
		timestampUpperBound,
		limit,
	)
	if err != nil {
		return rowsAffected, err
	}
	gcConfig.timestampLowerBound = newTimestampLowerBound
	return rowsAffected, nil
}

func runSystemLogGC(
	ctx context.Context, sqlServer *SQLServer, st *cluster.Settings, gcConfigs []systemLogGCConfig,
) {
	forSystemTenant := sqlServer.execCfg.Codec.ForSystemTenant()
	for i := range gcConfigs {
		gcConfig := &gcConfigs[i]
		if gcConfig.onlySystemTenant && !forSystemTenant {
			continue
		}

		if rowsAffected, err := runSystemLogGCForOneTable(ctx, sqlServer, st, gcConfig); err != nil {
			log.Warningf(ctx, "error garbage collecting %s.%s: %v", gcConfig.table, gcConfig.timestampCol, err)
		} else {
			log.Infof(ctx, "garbage collected %d rows from %s.%s", rowsAffected, gcConfig.table, gcConfig.timestampCol)
		}
	}
}

func getTablesToGC() []systemLogGCConfig {
	// NB: we need to reconstruct the slice anew every time
	// because the timestampLowerBound field is modified in-place
	// by the GC task.
	return []systemLogGCConfig{
		{true, "rangelog", "timestamp", rangeLogTTL, timeutil.Unix(0, 0)},
		{false, "eventlog", "timestamp", eventLogTTL, timeutil.Unix(0, 0)},
		{false, "web_sessions", "expiresAt", webSessionPurgeTTL, timeutil.Unix(0, 0)},
		{false, "web_sessions", "revokedAt", webSessionPurgeTTL, timeutil.Unix(0, 0)},
	}
}

// startSystemLogsGC starts a worker which periodically GCs system.rangelog
// and system.eventlog.
// The TTLs for each of these logs is retrieved from cluster settings.
//
// TODO(knz): This should be best replaced by SQL-level row TTL.
// See: https://github.com/cockroachdb/cockroach/issues/89453
func startSystemLogsGC(ctx context.Context, sqlServer *SQLServer) error {
	// systemLogsToGC stores the state of the GC loops,
	// including the lower bound for the deletion timestamps.
	systemLogsToGC := getTablesToGC()

	cfg := sqlServer.cfg
	st := sqlServer.execCfg.Settings

	return sqlServer.stopper.RunAsyncTask(ctx, "system-log-gc", func(ctx context.Context) {
		getPeriod := func() time.Duration {
			period := systemLogGCPeriod.Get(&st.SV)
			if period < 1*time.Second {
				// Clamp the period to 1 second to avoid busy looping.
				period = 1 * time.Second
			}
			if storeKnobs, ok := cfg.TestingKnobs.Store.(*kvserver.StoreTestingKnobs); ok && storeKnobs.SystemLogsGCPeriod != 0 {
				period = storeKnobs.SystemLogsGCPeriod
			}
			return period
		}
		period := getPeriod()
		var timer timeutil.Timer
		defer timer.Stop()
		// The very first period is 25% off the configured period, to
		// avoid a thundering herd effect on the system table between
		// nodes when multiple nodes are started simultaneously.
		timer.Reset(jitteredInterval(period))

		for ; ; timer.Reset(getPeriod()) {
			select {
			case <-timer.C:
				timer.Read = true

				// Do the work for all system tables.
				runSystemLogGC(ctx, sqlServer, st, systemLogsToGC)

				// If we are in a test, coordinate with the test.
				if storeKnobs, ok := cfg.TestingKnobs.Store.(*kvserver.StoreTestingKnobs); ok && storeKnobs.SystemLogsGCGCDone != nil {
					select {
					case storeKnobs.SystemLogsGCGCDone <- struct{}{}:
						// Step has made one step forward. We can continue to iterate.
					case <-sqlServer.stopper.ShouldQuiesce():
						// Test has finished.
						return
					case <-ctx.Done():
						// Test also has finished.
						return
					}
				}
			case <-sqlServer.stopper.ShouldQuiesce():
				// Server is shutting down.
				return
			case <-ctx.Done():
				// Something is telling us to go away.
				return
			}
		}
	})
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from the interval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*math_rand.Float64()))
}
