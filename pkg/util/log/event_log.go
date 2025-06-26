// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	eventLogWriterRegistry EventLogWriterRegistry
	defaultSEventOptions   = EventLogSettings{
		sev:   severity.INFO,
		depth: 1,
	}
)

// InitEventLogWriterRegistry initializes the global event log writer registry.
func InitEventLogWriterRegistry(registry EventLogWriterRegistry) {
	eventLogWriterRegistry = registry
}

// EventLogWriterRegistry defines the interface for a registry that manages
// EventLogWriter instances. It allows registering, removing, and retrieving
// writers based on server identifiers. One writer should be
// registered for each tenant.
type EventLogWriterRegistry interface {
	RegisterWriter(ctx context.Context, serverId serverident.ServerIdentifier, writer EventLogWriter)
	RemoveWriter(serverId serverident.ServerIdentifier)
	GetWriter(serverId serverident.ServerIdentifier) (EventLogWriter, bool)
}

// EventLogWriter defines the interface for writing structured event logs.
type EventLogWriter interface {
	Write(ctx context.Context, ev logpb.EventPayload, writeAsync bool)
}

// RegisterEventLogWriter registers a new EventLogWriter for the given server
// identifier.
func RegisterEventLogWriter(
	ctx context.Context, serverId serverident.ServerIdentifier, writer EventLogWriter,
) {
	if eventLogWriterRegistry == nil {
		if buildutil.CrdbTestBuild {
			panic("Structured event writer registry not set, cannot register writer")
		}
		Errorf(ctx, "Structured event writer registry not set, cannot register writer for serverId: %v", serverId.GetServerIdentificationPayload())
		return
	}

	eventLogWriterRegistry.RegisterWriter(ctx, serverId, writer)
}

// RemoveEventLogWriter removes the EventLogWriter for the given server
// identifier.
func RemoveEventLogWriter(serverId serverident.ServerIdentifier) {
	if eventLogWriterRegistry == nil {
		if buildutil.CrdbTestBuild {
			panic("Structured event writer registry not set, cannot remove writer")
		}
		Errorf(context.Background(), "Structured event writer registry not set, cannot remove writer for serverId: %v", serverId.GetServerIdentificationPayload())
		return
	}

	eventLogWriterRegistry.RemoveWriter(serverId)
}

type EventLogSettings struct {
	sev        logpb.Severity
	writeAsync bool
	depth      int
}

// StructuredEvent emits a structured event log of severity sev to the channel
// the provided event belongs to. This function only writes to logs, use
// EventLog if you want to also write to the system.eventlog table.
func StructuredEvent(ctx context.Context, sev logpb.Severity, event logpb.EventPayload) {
	// Note: we use depth 0 intentionally here, so that structured
	// events can be reliably detected (their source filename will
	// always be log/event_log.go).
	StructuredEventDepth(ctx, sev, 0, event)
}

// StructuredEventDepth emits a structured event log of severity sev and depth
// to the channel the provided event belongs to. This function only writes to
// logs, use EventLog if you want to also write to the system.eventlog table.
func StructuredEventDepth(
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

// EventLog emits a structured event log and writes it to the system.eventlog
// table.
func EventLog(
	ctx context.Context,
	id serverident.ServerIdentifier,
	event logpb.EventPayload,
	o ...StructuredEventSettingsFunc,
) {

	writer, ok := eventLogWriterRegistry.GetWriter(id)
	// There should be a writer registered for every tenant. If no writer is found
	// and this is a test build, panic to catch the error early. Otherwise
	// StructuredEvents won't be sent to a writer.
	if !ok {
		if buildutil.CrdbTestBuild {
			panic(fmt.Sprintf("couldn't find a registered writer for serverId: %+v", id.GetServerIdentificationPayload()))
		}
		VWarningf(context.Background(), 2, "couldn't find a registered writer for serverId: %+v", id.GetServerIdentificationPayload())
	}
	options := defaultSEventOptions
	for _, opt := range o {
		options = opt(options)
	}
	StructuredEventDepth(ctx, options.sev, options.depth, event)
	writer.Write(ctx, event, options.writeAsync)
}

type StructuredEventSettingsFunc func(EventLogSettings) EventLogSettings

func WithDepth(depth int) StructuredEventSettingsFunc {
	return func(o EventLogSettings) EventLogSettings {
		o.depth = depth
		return o
	}
}

func WithInfo() StructuredEventSettingsFunc {
	return func(o EventLogSettings) EventLogSettings {
		o.sev = severity.INFO
		return o

	}
}

func WithWarning() StructuredEventSettingsFunc {
	return func(o EventLogSettings) EventLogSettings {
		o.sev = severity.WARNING
		return o

	}
}

func WithError() StructuredEventSettingsFunc {
	return func(o EventLogSettings) EventLogSettings {
		o.sev = severity.ERROR
		return o

	}
}

func WriteAsync() StructuredEventSettingsFunc {
	return func(o EventLogSettings) EventLogSettings {
		o.writeAsync = true
		return o
	}
}
