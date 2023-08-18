// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import "github.com/cockroachdb/cockroach/pkg/settings"

// SQLStatsResponseMax controls the maximum number of statements and transactions returned by the
// CombinedStatements endpoint.
var SQLStatsResponseMax = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.stats.response.max",
	"the maximum number of statements and transaction stats returned in a CombinedStatements request",
	20000,
	settings.NonNegativeInt,
	settings.WithPublic)

// SQLStatsShowInternal controls if statistics for internal executions should be returned in sql stats APIs,
// including: CombinedStatementStats, ListSessions, and ListLocalSessions.
var SQLStatsShowInternal = settings.RegisterBoolSetting(
	settings.TenantWritable,
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
	settings.TenantWritable,
	"sql.stats.activity.ui.enabled",
	"enable the combined statistics endpoint to get data from the system activity tables",
	true)
