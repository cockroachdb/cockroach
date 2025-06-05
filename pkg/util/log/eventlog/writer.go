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
	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

type EventLogTestingKnobs struct {
	// SyncWrites causes events to be synchronously to the system.eventlog table.
	// This is needed for tests to provide a deterministic way to verify events
	// have been written.
	SyncWrites bool
}

func (e EventLogTestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = &EventLogTestingKnobs{}
var SystemTableEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.eventlog.enabled",
	"if set, logged notable events are also stored in the table system.eventlog",
	true,
	settings.WithPublic)

type registry struct {
	serverSinks syncutil.Map[serverident.ServerIdentificationPayload, log.EventLogWriter]
}

var _ log.EventLogWriterRegistry = &registry{}

func (e *registry) RegisterWriter(
	ctx context.Context, serverId serverident.ServerIdentifier, sink log.EventLogWriter,
) {
	e.serverSinks.Store(serverId.GetServerIdentificationPayload(), &sink)
}

func (e *registry) RemoveWriter(serverId serverident.ServerIdentifier) {
	e.serverSinks.Delete(serverId.GetServerIdentificationPayload())
}

func (e *registry) GetWriter(serverId serverident.ServerIdentifier) (log.EventLogWriter, bool) {
	sink, ok := e.serverSinks.Load(serverId.GetServerIdentificationPayload())
	return *sink, ok
}

// Writer implements log.EventLogWriter and writes events to the
// system.eventlog table.
type Writer struct {
	db             isql.DB
	stopper        *stop.Stopper
	writeAsync     bool
	ambientContext log.AmbientContext
	settings       *cluster.Settings
}

var _ log.EventLogWriter = &Writer{}

func NewWriter(
	db isql.DB,
	writeAsync bool,
	stopper *stop.Stopper,
	ambientContext log.AmbientContext,
	settings *cluster.Settings,
) *Writer {
	return &Writer{
		db:             db,
		stopper:        stopper,
		writeAsync:     writeAsync,
		ambientContext: ambientContext,
		settings:       settings,
	}
}

func Register(
	ctx context.Context,
	testKnob base.ModuleTestingKnobs,
	db isql.DB,
	stopper *stop.Stopper,
	ambientContext log.AmbientContext,
	settings *cluster.Settings,
) {
	var writeAsync = true
	if testKnob != nil {
		if eventLogKnob, ok := testKnob.(*EventLogTestingKnobs); ok {
			writeAsync = !eventLogKnob.SyncWrites
		}
	}

	writer := NewWriter(db, writeAsync, stopper, ambientContext, settings)
	log.RegisterEventLogWriter(ctx, &ambientContext, writer)
	stopper.AddCloser(stop.CloserFn(func() {
		log.RemoveEventLogWriter(&ambientContext)
	}))
}

// Writer implements the log.EventLogWriter interface. It writes events to the
// system.eventlog table. If writeAsync is true, it writes the events
// asynchronously, even if the writer is configured to write synchronously.
func (e *Writer) Write(ctx context.Context, ev logpb.EventPayload, writeAsync bool) {
	if !SystemTableEnabled.Get(&e.settings.SV) {
		return
	}

	if e.writeAsync || writeAsync {
		e.asyncWrite(ctx, ev)
	} else {
		e.syncWrite(ctx, ev)
	}
}

// asyncWrite is mostly copied from the sql/event_log.go file implementation of
// `asyncWriteToOtelAndSystemEventsTable`.
// TODO(obs): merge the code in the two files to avoid duplication.
func (e *Writer) asyncWrite(ctx context.Context, ev logpb.EventPayload) {
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
					return e.writeToSystemEventsTable(ctx, 1, query, args)
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

func (e *Writer) syncWrite(ctx context.Context, entry logpb.EventPayload) {
	query, args := e.prepareEventWrite([]logpb.EventPayload{entry})
	if err := e.writeToSystemEventsTable(ctx, 1, query, args); err != nil {
		log.Errorf(ctx, "Failed to write")
	}
}

func (e *Writer) writeToSystemEventsTable(
	ctx context.Context, numEntries int, query string, args []interface{},
) error {
	rows, err := e.db.Executor().ExecEx(ctx, "log-event", nil, sessiondata.NodeUserSessionDataOverride,
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

func (e *Writer) prepareEventWrite(
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
		reportingId := "0"
		if e.ambientContext.ServerIDs != nil {
			reportingId = e.ambientContext.ServerIDs.ServerIdentityString(serverident.IdentifyInstanceID)
		}
		args = append(
			args,
			timeutil.Unix(0, event.CommonDetails().Timestamp),
			eventType,
			reportingId,
			string(infoBytes),
		)
	}

	// In the common case where we have just 1 event, we want to skip
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

func init() {
	registry := &registry{}
	log.InitEventLogWriterRegistry(registry)
}
