// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import "github.com/cockroachdb/cockroach/pkg/settings"

// SQLStatsResponseMax controls the maximum number of statements and transactions returned by the
// CombinedStatements endpoint.
var SQLStatsResponseMax = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.response.max",
	"the maximum number of statements and transaction stats returned in a CombinedStatements request",
	20000,
	settings.NonNegativeInt,
	settings.WithPublic)

// SQLStatsShowInternal controls if statistics for internal executions should be returned in sql stats APIs,
// including: CombinedStatementStats, ListSessions, and ListLocalSessions.
var SQLStatsShowInternal = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.response.show_internal.enabled",
	"controls if statistics for internal executions should be returned by the CombinedStatements and if "+
		"internal sessions should be returned by the ListSessions endpoints. These endpoints are used to display "+
		"statistics on the SQL Activity pages",
	false,
	settings.WithPublic)

// StatsActivityUIEnabled controls if the combined statement stats uses
// the system.statement_activity and system.transaction_activity which
// acts as a cache storing the top queries from system.statement_statistics
// and system.transaction_statistics tables.
var StatsActivityUIEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.activity.ui.enabled",
	"enable the combined statistics endpoint to get data from the system activity tables",
	true)

// PersistedInsightsUIEnabled controls if the insights endpoint uses
// the persisted statement_execution_insights and transaction_execution_insights tables.
var PersistedInsightsUIEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.persisted_insights.ui.enabled",
	"enable the insights endpoint to get data from the persisted insights tables",
	false)

// DebugZipRedactAddressesEnabled guards whether hostname / ip address and other sensitive fields
// should be redacted in the debug zip
var DebugZipRedactAddressesEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"debug.zip.redact_addresses.enabled",
	"enables the redaction of hostnames and ip addresses in debug zip",
	false,
	settings.WithPublic,
	settings.WithReportable(true),
)
