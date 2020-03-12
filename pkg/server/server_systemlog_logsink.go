// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var eventlogSinkEnabled = settings.RegisterBoolSetting(
	"server.eventlogsink.enabled",
	`set to true to enable system.eventlog entries to be emitted to the node log`,
	false,
)

var eventlogSinkPeriod = settings.RegisterDurationSetting(
	"server.eventlogsink.period",
	`how frequently should the eventlog table be polled`,
	10*time.Second,
)

var eventlogSinkMaxEntries = settings.RegisterIntSetting(
	"server.eventlogsink.max_entries",
	`max number of eventlog entries to emit; set to 0 to emit all`,
	100,
)

var eventlogSincIncludeEvents = settings.RegisterStringSetting(
	"server.eventlogsink.include_events",
	`extract events matching this regex`,
	".*",
)

var eventlogSincExcludeEvents = settings.RegisterStringSetting(
	"server.eventlogsink.exclude_events",
	`exclude events matching this regex`,
	"comment|sequence",
)

func getEventlogSinkPeriod(sv *settings.Values) time.Duration {
	if !eventlogSinkEnabled.Get(sv) {
		// If log sink is not enabled, recheck after a minute.
		return time.Minute
	}

	p := eventlogSinkPeriod.Get(sv)
	if p < 100*time.Millisecond {
		return 100 * time.Millisecond
	}
	return p
}

// eventLogSink is just a wrapper around log methods to facilitate testing.
type eventLogSink interface {
	ReportEvent(ctx context.Context, ts time.Time, event string, reporter string, info string)
	Errorf(ctx context.Context, format string, args ...interface{})
}

type defaultLogSink struct{}

var _ eventLogSink = &defaultLogSink{}

func (d defaultLogSink) ReportEvent(
	ctx context.Context, ts time.Time, event string, reporter string, info string,
) {
	log.Infof(ctx, "system.eventlog:n=%s:%s:%s %s", reporter, event, ts, info)
}

func (d defaultLogSink) Errorf(ctx context.Context, format string, args ...interface{}) {
	log.Errorf(ctx, format, args...)
}

// sinkEventLog emits system.eventlog entries into the node log file.
// TODO(yevgeniy): Replace implementation with rangefeed based one, once rangefeeds
// support listening to system tables.
func sinkEventlog(
	ctx context.Context,
	sink eventLogSink,
	db *kv.DB,
	ex *sql.InternalExecutor,
	sv *settings.Values,
	logTimestamp time.Time,
) (time.Time, error) {
	maxEntries := eventlogSinkMaxEntries.Get(sv)
	var limit interface{}
	if maxEntries > 0 {
		limit = maxEntries
	}
	includeEvents := eventlogSincIncludeEvents.Get(sv)
	excludeEvents := eventlogSincExcludeEvents.Get(sv)

	const (
		tsCol = iota
		eventTypeCol
		reportingIDCol
		infoCol
	)

	const getEventsQuery = `
SELECT timestamp, "eventType", "reportingID", info
FROM [
  SELECT * FROM system.eventlog
  WHERE
    timestamp > $1 AND
    regexp_extract("eventType", $2) IS NOT NULL AND -- include events
    regexp_extract("eventType", $3) IS NULL         -- exclude events
  ORDER BY timestamp DESC
  LIMIT $4
] ORDER BY timestamp`

	maxTs := logTimestamp
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		rows, err := ex.QueryEx(
			ctx,
			"system.eventlog-sink",
			txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			getEventsQuery,
			logTimestamp,
			includeEvents,
			excludeEvents,
			limit,
		)

		if err != nil {
			return err
		}

		if int64(len(rows)) == maxEntries {
			log.Infof(ctx,
				"system.eventlog likely contains more than %d events emitted since %s; "+
					"consider increasing server.eventlogsink.max_entries setting, "+
					"or lowering the server.eventlogsink.period",
				maxEntries, logTimestamp,
			)
		}

		for _, row := range rows {
			ts, ok := row[tsCol].(*tree.DTimestamp)
			if !ok {
				return errors.Errorf("timestamp is of unknown type %T", rows[0])
			}

			maxTs = ts.Time
			sink.ReportEvent(
				ctx, maxTs, row[eventTypeCol].String(), row[reportingIDCol].String(), row[infoCol].String())
		}
		return nil
	})

	return maxTs, err
}

func (s *Server) startEventlogSink(ctx context.Context) {
	logTimestamp := time.Now()

	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		sv := &s.ClusterSettings().SV
		for {
			select {
			case <-s.stopper.ShouldStop():
				return
			case <-time.After(getEventlogSinkPeriod(sv)):
				if eventlogSinkEnabled.Get(sv) {
					newTs, err := sinkEventlog(ctx, defaultLogSink{}, s.db, s.internalExecutor, sv, logTimestamp)
					if err == nil {
						logTimestamp = newTs
					} else {
						log.Errorf(ctx, "failed to sink system.eventlog records: %v", err)
					}
				}
			}
		}
	})
}
