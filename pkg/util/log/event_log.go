// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type EventLogSink interface {
	WriteEvent(ctx context.Context, ev logpb.EventPayload)
}

var eventLogSink EventLogSink

func RegisterEventLogSink(sink EventLogSink) {
	eventLogSink = sink
}

type StructuredEventOption struct {
	sev          logpb.Severity
	writeToTable bool
	depth        int
}

type StructuredEventOptFunc func(*StructuredEventOption)

func WithDepth(depth int) StructuredEventOptFunc {
	return func(o *StructuredEventOption) {
		o.depth = depth
	}
}

func WithInfo() StructuredEventOptFunc {
	return func(o *StructuredEventOption) {
		o.sev = severity.INFO
	}
}

func WithWarning() StructuredEventOptFunc {
	return func(o *StructuredEventOption) {
		o.sev = severity.WARNING
	}
}

func WithError() StructuredEventOptFunc {
	return func(o *StructuredEventOption) {
		o.sev = severity.ERROR
	}
}

func WithFatal() StructuredEventOptFunc {
	return func(o *StructuredEventOption) {
		o.sev = severity.FATAL
	}
}

func WithWriteToTable(writeToTable bool) StructuredEventOptFunc {
	return func(o *StructuredEventOption) {
		Infof(context.Background(), "writting event to eventlog table? %t", writeToTable)
		o.writeToTable = writeToTable
	}
}

// StructuredEvent emits a structured event log of severity sev to the channel the provided
// event belongs to.
// Deprecated: use SEvent instead
func StructuredEvent(ctx context.Context, sev logpb.Severity, event logpb.EventPayload) {
	// Note: we use depth 0 intentionally here, so that structured
	// events can be reliably detected (their source filename will
	// always be log/event_log.go).
	structuredEventDepth(ctx, sev, 0, event)
}

// StructuredEventDepth emits a structured event log of severity sev and depth to the channel the provided
// event belongs to.
// Deprecated: use SEvent instead
func StructuredEventDepth(
	ctx context.Context, sev logpb.Severity, depth int, event logpb.EventPayload,
) {
	structuredEventDepth(ctx, sev, depth+1, event)
}

// structuredEventDepth emits a structured event log of severity sev and depth to the channel the provided
// event belongs to.
func structuredEventDepth(
	ctx context.Context, sev logpb.Severity, depth int, event logpb.EventPayload,
) {
	// Populate the missing common fields.
	common := event.CommonDetails()
	if common.Timestamp == 0 {
		common.Timestamp = timeutil.Now().UnixNano()
	}
	if len(common.EventType) == 0 {
		common.EventType = logpb.GetEventTypeName(event)
	}

	entry := makeStructuredEntry(ctx,
		sev,
		event.LoggingChannel(),
		depth,
		event)

	if sp := getSpan(ctx); sp != nil {
		// Prevent `entry` from moving to the heap when this branch is not taken.
		heapEntry := entry
		eventInternal(sp, entry.sev >= severity.ERROR, &heapEntry)
	}

	logger := logging.getLogger(entry.ch)
	logger.outputLogEntry(entry)
}

// TODO (kyle.wong): Do we need to add support for "logDevIfVerbosity" that currently exists in sql/event_log?
//  I'm assuming we do to keep backwards compatibility.

func SEvent(ctx context.Context, event logpb.EventPayload, o ...StructuredEventOptFunc) {
	options := &StructuredEventOption{
		sev:          severity.INFO,
		writeToTable: false,
		depth:        1,
	}
	for _, opt := range o {
		opt(options)
	}

	// Note: we use depth 0 intentionally here, so that structured
	// events can be reliably detected (their source filename will
	// always be log/event_log.go).
	structuredEventDepth(ctx, options.sev, options.depth, event)
	Infof(ctx, "writting event to eventlog table? %t", options.writeToTable)
	if options.writeToTable {
		if eventLogSink == nil {
			Errorf(ctx, "unable to write to system.eventlog table, no sink registered")
			return
		}
		eventLogSink.WriteEvent(ctx, event)
	}
}
