// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// SQLStatsFlushInterval is the cluster setting that controls how often the SQL
// stats are flushed to system table.
var SQLStatsFlushInterval = settings.RegisterDurationSetting(
	"sql.stats.flush.interval",
	"this interval controls how often SQL stats are flushed to system table,"+
		"it's default to 1 hour",
	time.Hour,
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
).WithPublic()

// SQLStatsFlushEnabled is the cluster setting that controls if the sqlstats
// subsystem persists the statistics into system table.
var SQLStatsFlushEnabled = settings.RegisterBoolSetting(
	"sql.stats.flush.enabled",
	"this controls whether the SQL statistics are being flushed to system table",
	false, /* defaultValue */
).WithPublic()
