// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
)

var (
	OpsLogger             = StructuredLogger{baseStructuredLogger: bsl, channel: channel.OPS}
	EventsLogger          = StructuredLogger{baseStructuredLogger: bsl, channel: channel.STRUCTURED_EVENTS}
	DevLogger             = StructuredLogger{baseStructuredLogger: bsl, channel: channel.DEV}
	HealthLogger          = StructuredLogger{baseStructuredLogger: bsl, channel: channel.HEALTH}
	StorageLogger         = StructuredLogger{baseStructuredLogger: bsl, channel: channel.STORAGE}
	SessionsLogger        = StructuredLogger{baseStructuredLogger: bsl, channel: channel.SESSIONS}
	SqlSchemaLogger       = StructuredLogger{baseStructuredLogger: bsl, channel: channel.SQL_SCHEMA}
	UserAdminLogger       = StructuredLogger{baseStructuredLogger: bsl, channel: channel.USER_ADMIN}
	PrivilegesLogger      = StructuredLogger{baseStructuredLogger: bsl, channel: channel.PRIVILEGES}
	SensitiveAccessLogger = StructuredLogger{baseStructuredLogger: bsl, channel: channel.SENSITIVE_ACCESS}
	SqlExecLogger         = StructuredLogger{baseStructuredLogger: bsl, channel: channel.SQL_EXEC}
	SqlPerfLogger         = StructuredLogger{baseStructuredLogger: bsl, channel: channel.SQL_PERF}
	SqlInternalPerfLogger = StructuredLogger{baseStructuredLogger: bsl, channel: channel.SQL_INTERNAL_PERF}
	TelemetryLogger       = StructuredLogger{baseStructuredLogger: bsl, channel: channel.TELEMETRY}
	KvLogger              = StructuredLogger{baseStructuredLogger: bsl, channel: channel.KV_DISTRIBUTION}
)

// sqlStructuredLogsEnabled enables or disables structured logging
var sqlStructuredLogsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.log.structured_logs.enabled",
	"enables the logging of all structured log events",
	true)

type ChannelStructuredLogger interface {
	Info(ctx context.Context, meta log.StructuredLogMeta, payload any)
}

type baseStructuredLogger struct {
	clusterSettings *cluster.Settings
}

func (b *baseStructuredLogger) structuredLogsEnabled() bool {
	return sqlStructuredLogsEnabled.Get(&b.clusterSettings.SV)
}

func Init(settings *cluster.Settings) {
	bsl.clusterSettings = settings
}

// TODO[kyle] do we want to do this or would we rather it panic from a nil pointer?
var bsl = &baseStructuredLogger{clusterSettings: cluster.MakeClusterSettings()}

type StructuredLogger struct {
	*baseStructuredLogger
	channel log.Channel
}

func (sl *StructuredLogger) Channel() log.Channel {
	return sl.channel
}

func (sl *StructuredLogger) SetChannel(channel log.Channel) {
	sl.channel = channel
}

var _ ChannelStructuredLogger = (*StructuredLogger)(nil)

func (sl *StructuredLogger) Info(ctx context.Context, meta log.StructuredLogMeta, payload any) {
	if sl.structuredLogsEnabled() {
		log.Structured(ctx, sl.channel, severity.INFO, meta, payload)
	}
}
