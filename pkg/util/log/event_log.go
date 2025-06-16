// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	sinkRegistry         SEventSinkRegistry
	defaultSEventOptions = StructuredEventOption{
		sev:          severity.INFO,
		writeToTable: false,
		depth:        1,
	}
)

func InitSink(sink SEventSinkRegistry) {
	sinkRegistry = sink
}

type SEventSinkRegistry interface {
	RegisterSink(ctx context.Context, ambientContext AmbientContext, sink SEventSink)
	DeregisterSink(ambientContext AmbientContext)
	GetSink(ambientContext AmbientContext) (SEventSink, bool)
}

type SEventSink interface {
	WriteEvent(ctx context.Context, ev logpb.EventPayload, writeAsync bool)
}

func RegisterEventLogSink(ctx context.Context, ambientContext AmbientContext, sink SEventSink) {
	if sinkRegistry == nil {
		if buildutil.CrdbTestBuild {
			panic("OH NO")
		}
		Error(ctx, "Structured event sink registry not set, cannot register sink")
		return
	}

	sinkRegistry.RegisterSink(ctx, ambientContext, sink)
}

func DeregisterEventLogSink(ambientContext AmbientContext) {
	if sinkRegistry == nil {
		if buildutil.CrdbTestBuild {
			panic("OH NO")
		}
		Errorf(context.Background(), "Structured event sink registry not set, cannot deregister sink")
		return
	}

	sinkRegistry.DeregisterSink(ambientContext)
}

type StructuredEventOption struct {
	sev          logpb.Severity
	writeToTable bool
	writeAsync   bool
	depth        int
}

// StructuredEvent emits a structured event log of severity sev to the channel the provided
// event belongs to.
// Deprecated: use SEventLogger.Log instead
func StructuredEvent(ctx context.Context, sev logpb.Severity, event logpb.EventPayload) {
	// Note: we use depth 0 intentionally here, so that structured
	// events can be reliably detected (their source filename will
	// always be log/event_log.go).
	structuredEventDepth(ctx, sev, 0, event)
}

// StructuredEventDepth emits a structured event log of severity sev and depth to the channel the provided
// event belongs to.
// Deprecated: use SEventLogger.Log instead
func StructuredEventDepth(
	ctx context.Context, sev logpb.Severity, depth int, event logpb.EventPayload,
) {
	structuredEventDepth(ctx, sev, depth, event)
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
		depth+1,
		event)

	if sp := getSpan(ctx); sp != nil {
		// Prevent `entry` from moving to the heap when this branch is not taken.
		heapEntry := entry
		eventInternal(sp, entry.sev >= severity.ERROR, &heapEntry)
	}

	logger := logging.getLogger(entry.ch)
	logger.outputLogEntry(entry)
}

type SEventLogger struct {
	sink           SEventSink
	defaultOptions StructuredEventOption
}

func NewSEventLogger(ac AmbientContext, o ...StructuredEventOptFunc) SEventLogger {
	defaultOptions := defaultSEventOptions
	for _, opt := range o {
		defaultOptions = opt(defaultSEventOptions)
	}

	sink, ok := sinkRegistry.GetSink(ac)
	if !ok {
		if buildutil.CrdbTestBuild {
			panic(fmt.Sprintf("couldn't find a registered sink for serverId: %+v", ac.ServerIDs))
		}
		return SEventLogger{sink: nil, defaultOptions: defaultOptions}
	}
	return SEventLogger{sink: sink, defaultOptions: defaultOptions}
}

func (S *SEventLogger) StructuredEvent(
	ctx context.Context, event logpb.EventPayload, o ...StructuredEventOptFunc,
) {
	options := S.defaultOptions
	for _, opt := range o {
		options = opt(options)
	}
	structuredEventDepth(ctx, options.sev, options.depth, event)
	if options.writeToTable && S.sink != nil {
		if sinkRegistry == nil {
			if buildutil.CrdbTestBuild {
				panic("OH NO")
			}
			return
		}

		S.sink.WriteEvent(ctx, event, options.writeAsync)
	}
}

type StructuredEventOptFunc func(StructuredEventOption) StructuredEventOption

func WithDepth(depth int) StructuredEventOptFunc {
	return func(o StructuredEventOption) StructuredEventOption {
		o.depth = depth
		return o
	}
}

func WithInfo() StructuredEventOptFunc {
	return func(o StructuredEventOption) StructuredEventOption {
		o.sev = severity.INFO
		return o

	}
}

func WithWarning() StructuredEventOptFunc {
	return func(o StructuredEventOption) StructuredEventOption {
		o.sev = severity.WARNING
		return o

	}
}

func WithError() StructuredEventOptFunc {
	return func(o StructuredEventOption) StructuredEventOption {
		o.sev = severity.ERROR
		return o

	}
}

func WithFatal() StructuredEventOptFunc {
	return func(o StructuredEventOption) StructuredEventOption {
		o.sev = severity.FATAL
		return o

	}
}

func WriteAsync() StructuredEventOptFunc {
	return func(o StructuredEventOption) StructuredEventOption {
		o.writeAsync = true
		return o
	}
}

func WithWriteToTable(writeToTable bool) StructuredEventOptFunc {
	return func(o StructuredEventOption) StructuredEventOption {
		Infof(context.Background(), "writting event to eventlog table? %t", writeToTable)
		o.writeToTable = writeToTable
		return o
	}
}
