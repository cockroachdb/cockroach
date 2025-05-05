// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eventlog

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

var eventLogSystemTableEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.eventlog.enabled",
	"if set, logged notable events are also stored in the table system.eventlog",
	true,
	settings.WithPublic)

type EventTableSink struct {
	reportingID    base.SQLInstanceID
	db             isql.DB
	stopper        *stop.Stopper
	writeAsync     bool
	ambientContext log.AmbientContext
	settings       *cluster.Settings
}

var _ log.EventLogSink = &EventTableSink{}

func NewEventTableSink(
	reportingID base.SQLInstanceID,
	db isql.DB,
	writeAsync bool,
	stopper *stop.Stopper,
	ambientContext log.AmbientContext,
	settings *cluster.Settings,
) *EventTableSink {
	return &EventTableSink{
		reportingID:    reportingID,
		db:             db,
		stopper:        stopper,
		writeAsync:     writeAsync,
		ambientContext: ambientContext,
		settings:       settings,
	}
}

func (e *EventTableSink) WriteEvent(ctx context.Context, ev logpb.EventPayload) {
	if !eventLogSystemTableEnabled.Get(&e.settings.SV) {
		return
	}

	if e.writeAsync {
		log.Infof(ctx, "ASYNC Writing event to eventlog table")
		e.asyncWrite(ctx, ev)
	} else {
		log.Infof(ctx, "SYNC Writing event to eventlog table")
		e.syncWrite(ctx, ev)
	}
}

func (e *EventTableSink) asyncWrite(ctx context.Context, ev logpb.EventPayload) {
	// perAttemptTimeout is the maximum amount of time to wait on each
	// eventlog write attempt.
	const perAttemptTimeout time.Duration = 5 * time.Second
	// maxAttempts is the maximum number of attempts to write an
	// eventlog entry.
	const maxAttempts = 10

	stopper := e.stopper
	origCtx := ctx
	entries := []logpb.EventPayload{ev}
	if err := stopper.RunAsyncTask(
		// Note: we don't want to inherit the cancellation of the parent
		// context. The SQL statement that causes the eventlog entry may
		// terminate (and its context cancelled) before the eventlog entry
		// gets written.
		context.Background(), "record-events", func(ctx context.Context) {
			ctx, span := e.ambientContext.AnnotateCtxWithSpan(ctx, "record-events")
			defer span.Finish()

			// Copy the tags from the original query (which will contain
			// things like username, current internal executor context, etc).
			ctx = logtags.AddTags(ctx, logtags.FromContext(origCtx))

			// Stop writing the event when the server shuts down.
			ctx, stopCancel := stopper.WithCancelOnQuiesce(ctx)
			defer stopCancel()

			// Prepare the data to send.
			query, args := e.prepareEventWrite(entries)

			// We use a retry loop in case there are transient
			// non-retriable errors on the cluster during the table write.
			// (retriable errors are already processed automatically
			// by db.Txn)
			retryOpts := base.DefaultRetryOptions()
			retryOpts.Closer = ctx.Done()
			retryOpts.MaxRetries = int(maxAttempts)
			for r := retry.Start(retryOpts); r.Next(); {
				// Don't try too long to write if the system table is unavailable.
				if err := timeutil.RunWithTimeout(ctx, "record-events", perAttemptTimeout, func(ctx context.Context) error {
					return e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
						return e.writeToSystemEventsTable(ctx, txn, len(entries), query, args)
					})
				}); err != nil {
					log.Ops.Warningf(ctx, "unable to save %d entries to system.eventlog: %v", len(entries), err)
				} else {
					break
				}
			}
		}); err != nil {
		expectedStopperError := errors.Is(err, stop.ErrThrottled) || errors.Is(err, stop.ErrUnavailable)
		if !expectedStopperError {
			// RunAsyncTask only returns an error not listed above
			// if its context was canceled, and we're using the
			// background context here.
			err = errors.NewAssertionErrorWithWrappedErrf(err, "unexpected stopper error")
		}
		log.Warningf(ctx, "failed to start task to save %d events in eventlog: %v", len(entries), err)
	}
}

func (e *EventTableSink) syncWrite(ctx context.Context, entry logpb.EventPayload) {
	if err := e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		query, args := e.prepareEventWrite([]logpb.EventPayload{entry})
		if err := e.writeToSystemEventsTable(ctx, txn, 1, query, args); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Errorf(ctx, "Failed to write")
	}
}

func (*EventTableSink) writeToSystemEventsTable(
	ctx context.Context, txn isql.Txn, numEntries int, query string, args []interface{},
) error {
	rows, err := txn.ExecEx(
		ctx, "log-event", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		query, args...,
	)
	if err != nil {
		log.Errorf(ctx, "Failed to write to system.eventlog: %v", err)
		return err
	}
	if rows != numEntries {
		log.Errorf(ctx, "Wrote %d events to system.eventlog", rows)
		return errors.AssertionFailedf("%d rows affected by log insertion; expected %d rows affected", rows, numEntries)
	}
	log.Infof(ctx, "Wrote %d events to system.eventlog", numEntries)
	return nil
}

func (e *EventTableSink) prepareEventWrite(
	entries []logpb.EventPayload,
) (query string, args []interface{}) {
	const colsPerEvent = 4
	// Note: we insert the value zero as targetID because sadly this
	// now-deprecated column has a NOT NULL constraint.
	// TODO(knz): Add a migration to remove the column altogether.
	const baseQuery = `
INSERT INTO system.eventlog (
  timestamp, "eventType", "reportingID", info, "targetID"
)
VALUES($1, $2, $3, $4, 0)`
	args = make([]interface{}, 0, len(entries)*colsPerEvent)

	for i := 0; i < len(entries); i++ {
		event := entries[i]

		infoBytes := redact.RedactableBytes("{")
		_, infoBytes = event.AppendJSONFields(false /* printComma */, infoBytes)
		infoBytes = append(infoBytes, '}')
		// In the system.eventlog table, we do not use redaction markers.
		// (compatibility with previous versions of CockroachDB.)
		infoBytes = infoBytes.StripMarkers()
		eventType := event.CommonDetails().EventType
		args = append(
			args,
			timeutil.Unix(0, event.CommonDetails().Timestamp),
			eventType,
			e.reportingID,
			string(infoBytes),
		)
	}

	// In the common case where we have just 1 event, we want to skeep
	// the extra heap allocation and buffer operations of the loop
	// below. This is an optimization.
	query = baseQuery
	if len(entries) > 1 {
		// Extend the query with additional VALUES clauses for all the
		// events after the first one.
		var completeQuery strings.Builder
		completeQuery.WriteString(baseQuery)

		for i := range entries[1:] {
			placeholderNum := 1 + colsPerEvent*(i+1)
			fmt.Fprintf(&completeQuery, ", ($%d, $%d, $%d, $%d, 0)",
				placeholderNum, placeholderNum+1, placeholderNum+2, placeholderNum+3)
		}
		query = completeQuery.String()
	}

	return query, args
}
