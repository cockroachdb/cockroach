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
).WithPublic()

// SQLStatsShowInternal controls if statistics for internal executions should be returned by the
// CombinedStatements endpoint.
var SQLStatsShowInternal = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.stats.response.show_internal.enabled",
	"controls if statistics for internal executions should be returned by the CombinedStatements endpoint. This "+
		"endpoint is used to display statistics on the Statement and Transaction fingerprint pages under SQL Activity",
	false,
).WithPublic()
