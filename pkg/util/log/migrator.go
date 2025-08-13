// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
)

// ChannelCompatibilityModeEnabled controls whether certain logs are routed
// to newly defined logging channels or continue to use their original ones.
var ChannelCompatibilityModeEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"log.channel_compatibility_mode.enabled",
	"when true, logs will continue to log to the expected logging channels; "+
		"when false, logs will be moved to new logging channels as part of a "+
		"logging channel consolidation effort",
	metamorphic.ConstantWithTestBool("log.channel_compatibility_mode.enabled", true),
)

func ShouldMigrateEvent(sv *settings.Values) bool {
	return !ChannelCompatibilityModeEnabled.Get(sv)
}

// StructuredEventMigrator handles conditional routing of structured events
// between old and new logging channels based on a shouldMigrate function.
type StructuredEventMigrator struct {
	shouldMigrate func() bool
	newChannel    Channel
}

// NewStructuredEventMigrator creates a new StructuredEventMigrator that routes
// structured events to either the specified new channel (when shouldMigrate returns
// true) or to the event's original channel (when shouldMigrate returns false).
func NewStructuredEventMigrator(
	shouldMigrate func() bool, newChannel Channel,
) StructuredEventMigrator {
	return StructuredEventMigrator{
		shouldMigrate: shouldMigrate,
		newChannel:    newChannel,
	}
}

// StructuredEvent logs a structured event using the migrator's routing logic.
func (sem StructuredEventMigrator) StructuredEvent(
	ctx context.Context, sev logpb.Severity, event logpb.EventPayload,
) {
	sem.structuredEventDepth(ctx, sev, 1, event)
}

// StructuredEventDepth logs a structured event with a custom stack depth
// for accurate caller identification in logs.
func (sem StructuredEventMigrator) StructuredEventDepth(
	ctx context.Context, sev logpb.Severity, depth int, event logpb.EventPayload,
) {
	sem.structuredEventDepth(ctx, sev, depth+1, event)
}

// structuredEventDepth is the internal implementation that performs the actual
// channel routing based on the shouldMigrate function value.
func (sem StructuredEventMigrator) structuredEventDepth(
	ctx context.Context, sev logpb.Severity, depth int, event logpb.EventPayload,
) {
	if sem.shouldMigrate() {
		structuredEventDepth(ctx, sev, depth+1, sem.newChannel, event)
	} else {
		structuredEventDepth(ctx, sev, depth+1, event.LoggingChannel(), event)

	}
}

// Migrator handles conditional routing of formatted log messages between
// deprecated and new logging channels based on a migration setting.
type Migrator struct {
	shouldMigrate func() bool
	oldChannel    Channel
	newChannel    Channel
}

// NewMigrator creates a new Migrator that routes log messages between old and new
// channels based on the shouldMigrate function.
func NewMigrator(shouldMigrate func() bool, oldChannel Channel, newChannel Channel) Migrator {
	return Migrator{shouldMigrate: shouldMigrate, oldChannel: oldChannel, newChannel: newChannel}
}

// logfDepth is the internal helper that routes log messages to either the
// new channel (when shouldMigrate returns true) or old channel (when shouldMigrate returns false).
func (m Migrator) logfDepth(
	ctx context.Context, sev Severity, depth int, format string, args ...interface{},
) {
	if m.shouldMigrate() {
		logfDepth(ctx, depth+1, sev, m.newChannel, format, args...)
	} else {
		logfDepth(ctx, depth+1, sev, m.oldChannel, format, args...)
	}
}

// Infof logs an info-level message using the migrator's routing logic.
func (m Migrator) Infof(ctx context.Context, format string, args ...interface{}) {
	m.logfDepth(ctx, severity.INFO, 1, format, args...)
}

// Warningf logs a warning-level message using the migrator's routing logic.
func (m Migrator) Warningf(ctx context.Context, format string, args ...interface{}) {
	m.logfDepth(ctx, severity.WARNING, 1, format, args...)
}

// Errorf logs an error-level message using the migrator's routing logic.
func (m Migrator) Errorf(ctx context.Context, format string, args ...interface{}) {
	m.logfDepth(ctx, severity.ERROR, 1, format, args...)
}

// Fatalf logs a fatal-level message using the migrator's routing logic.
func (m Migrator) Fatalf(ctx context.Context, format string, args ...interface{}) {
	m.logfDepth(ctx, severity.FATAL, 1, format, args...)
}
